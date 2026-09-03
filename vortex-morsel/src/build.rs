// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Building an [`ExecPlan`] from a layout tree and a query.
//!
//! The plan is the immutable half of the design's split: one blueprint per scan. Each worker
//! instantiates one thread-local [`Arena`], whose node state survives IO suspension and is recycled
//! across that worker's morsels without crossing a thread boundary.
//!
//! Two traits make the plan open without making it dynamic:
//!
//! * [`LayoutPlanner`] turns one kind of stored layout into nodes. The built-in planners cover
//!   flat, chunked, struct, dictionary, and the transparent zoned wrappers; a new layout registers
//!   a planner instead of editing a match. Unsupported layouts are build errors rather than silent
//!   fallbacks, so an unsupported query can never be timed as if the executor had run it.
//! * [`NodeBlueprint`] is the immutable description of one node. Planners push blueprints; every
//!   worker arena instantiates them; the scheduler only asks a blueprint which stored unit it
//!   reads.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::dtype::DType;
use vortex_array::dtype::Field;
use vortex_array::dtype::FieldName;
use vortex_array::dtype::FieldNames;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::StructFields;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::Expression;
use vortex_array::expr::analysis::referenced_field_paths;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_layout::LayoutRef;
use vortex_layout::layouts::struct_::Struct;
use vortex_mask::Mask;

use crate::io::IoKey;
use crate::layouts::ChunkedPlanner;
use crate::layouts::DictPlanner;
use crate::layouts::FlatPlanner;
use crate::layouts::StructPlanner;
use crate::layouts::ZonedPlanner;
use crate::node::Arena;
use crate::node::ExecNode;
use crate::node::NodeId;
use crate::nodes::ConjunctMode;
use crate::nodes::ConjunctSpec;
use crate::nodes::FilterSpec;
use crate::nodes::StructSpec;

/// The immutable blueprint of one node: everything a worker needs to instantiate it.
pub trait NodeBlueprint: Send + Sync {
    /// Create this node's mutable state for one worker arena. `id` is the node's own index.
    fn instantiate(&self, id: NodeId) -> Box<dyn ExecNode>;

    /// The stored units this node reads, each with the root rows whose morsels use it.
    ///
    /// Lease counts and lookahead are computed from this alone, so a node that reads storage
    /// must report every unit and a node that only combines children reports nothing. The
    /// runtime half of the contract is on the node: for every morsel whose range overlaps a
    /// reported unit, the instantiated node registers that unit during planning and releases it
    /// exactly once at retire, whether or not it ended up using the bytes.
    fn stored_uses(&self) -> Vec<(IoKey, Range<u64>)> {
        Vec::new()
    }
}

/// Plans the nodes for one kind of stored layout.
///
/// Planners are consulted in registration order and the first whose [`handles`] accepts the
/// layout owns it. A planner that only wraps another layout forwards through [`LayoutCx::child`];
/// one that reads storage pushes a blueprint whose [`NodeBlueprint::stored_uses`] names the unit.
///
/// [`handles`]: LayoutPlanner::handles
pub trait LayoutPlanner: Send + Sync {
    /// Whether this planner owns `layout`.
    fn handles(&self, layout: &LayoutRef) -> bool;

    /// Record the root rows at which `layout` starts fresh stored chunks.
    ///
    /// This runs before any node exists, to cut morsels, and must not materialize indivisible
    /// children. `root_offset` is the root-coordinate row of the layout's first row.
    fn natural_splits(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut SplitCx<'_>,
    ) -> VortexResult<()>;

    /// Append the nodes for `layout` to the plan and return the subtree root.
    fn plan(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut LayoutCx<'_>,
    ) -> VortexResult<NodeId>;
}

/// The ordered set of planners a plan is built with.
#[derive(Clone)]
pub struct LayoutPlanners {
    planners: Vec<Arc<dyn LayoutPlanner>>,
}

impl Default for LayoutPlanners {
    /// The built-in planners: zoned wrappers, flat, dictionary, struct, chunked.
    fn default() -> Self {
        Self {
            planners: vec![
                Arc::new(ZonedPlanner),
                Arc::new(FlatPlanner),
                Arc::new(DictPlanner),
                Arc::new(StructPlanner),
                Arc::new(ChunkedPlanner),
            ],
        }
    }
}

impl LayoutPlanners {
    /// No planners at all; every layout is an error until some are added.
    pub fn empty() -> Self {
        Self {
            planners: Vec::new(),
        }
    }

    /// Add a planner ahead of the existing ones, so it takes precedence for the layouts it handles.
    pub fn with(mut self, planner: Arc<dyn LayoutPlanner>) -> Self {
        self.planners.insert(0, planner);
        self
    }

    /// Build an execution plan for `layout` under `projection` and `filter`.
    ///
    /// The expressions are *unbound*: each conjunct and the projection are re-bound against the
    /// narrowed struct dtype of just the fields they reference, which is what lets a subtree read
    /// only its own columns without any expression rewriting.
    pub fn build_plan(
        &self,
        layout: &LayoutRef,
        projection: &Expression,
        filter: Option<&Expression>,
        mode: ConjunctMode,
    ) -> VortexResult<ExecPlan> {
        build_plan_inner(self, layout, projection, filter, mode, None)
    }

    /// Build a plan that materializes only layout chunks intersecting `ranges`.
    ///
    /// The ranges use root row coordinates and must be sorted, non-overlapping, non-empty, and
    /// within the layout row count. The resulting plan rejects scans outside those ranges.
    pub fn build_plan_for_ranges(
        &self,
        layout: &LayoutRef,
        projection: &Expression,
        filter: Option<&Expression>,
        mode: ConjunctMode,
        ranges: &[Range<u64>],
    ) -> VortexResult<ExecPlan> {
        if ranges.is_empty() {
            vortex_bail!("a range-scoped morsel plan requires at least one range");
        }
        let mut previous_end = 0;
        for (idx, range) in ranges.iter().enumerate() {
            if range.start >= range.end {
                vortex_bail!("planned range must be non-empty, got {range:?}");
            }
            if range.end > layout.row_count() {
                vortex_bail!(
                    "planned range {range:?} exceeds layout row count {}",
                    layout.row_count()
                );
            }
            if idx > 0 && range.start < previous_end {
                vortex_bail!("planned ranges must be sorted and non-overlapping");
            }
            previous_end = range.end;
        }
        build_plan_inner(
            self,
            layout,
            projection,
            filter,
            mode,
            Some(Arc::from(ranges)),
        )
    }

    /// Compute natural morsel ranges for the referenced columns without materializing
    /// indivisible chunk children.
    ///
    /// This mirrors the lazy V1 split walk: chunk row counts and indivisibility come from
    /// serialized child metadata, so an all-flat chunked column contributes its boundaries
    /// without constructing every child layout.
    pub fn natural_morsels_for(
        &self,
        layout: &LayoutRef,
        projection: &Expression,
        filter: Option<&Expression>,
        target_rows: u64,
    ) -> VortexResult<Vec<Range<u64>>> {
        let root_fields = struct_root(layout)?;

        let mut names = referenced_names(projection, layout.dtype(), root_fields)?;
        if let Some(filter) = filter {
            for name in referenced_names(filter, layout.dtype(), root_fields)? {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
            names.sort_by_key(|name| root_fields.find(name).unwrap_or(usize::MAX));
        }

        let mut splits = Vec::new();
        let mut cx = SplitCx {
            planners: self,
            splits: &mut splits,
        };
        for name in names {
            let idx = root_fields
                .find(&name)
                .ok_or_else(|| vortex_err!("field {name} not found in the scan dtype"))?;
            let field = layout
                .slot(idx + 1)?
                .ok_or_else(|| vortex_err!("struct layout has no child for field {idx}"))?;
            cx.child(&field, 0)?;
        }
        Ok(cut_morsels(
            &finish_splits(splits, layout.row_count()),
            target_rows,
        ))
    }

    fn find(&self, layout: &LayoutRef, root_offset: u64) -> VortexResult<Arc<dyn LayoutPlanner>> {
        self.planners
            .iter()
            .find(|planner| planner.handles(layout))
            .cloned()
            .ok_or_else(|| {
                vortex_err!(
                    "the morsel executor has no planner for layout {} at row offset {root_offset}",
                    layout.encoding_id()
                )
            })
    }
}

/// What a planner may do while recording natural splits.
pub struct SplitCx<'a> {
    planners: &'a LayoutPlanners,
    splits: &'a mut Vec<u64>,
}

impl SplitCx<'_> {
    /// Record that a fresh stored chunk starts at root row `root_row`.
    pub fn split_at(&mut self, root_row: u64) {
        self.splits.push(root_row);
    }

    /// Record the splits of a child layout whose first row is at `root_offset`.
    pub fn child(&mut self, layout: &LayoutRef, root_offset: u64) -> VortexResult<()> {
        let planner = self.planners.find(layout, root_offset)?;
        planner.natural_splits(layout, root_offset, self)
    }
}

/// What a planner may do while building nodes.
///
/// A context carries a *lease scope*: the root rows whose morsels use whatever is planned under
/// it. Normally a stored unit is used by the morsels covering its own rows, but a dictionary's
/// values are used by every morsel of the codes' range, so the dictionary planner scopes the
/// values subtree to that range. Scoped subtrees never cut morsels and are never pruned by the
/// planned ranges. Scopes only widen: a scope opened inside another keeps the outer one, because
/// the outer range is already in root rows while an inner layout's own rows are not.
pub struct LayoutCx<'a> {
    builder: &'a mut Builder,
    lease: Option<Range<u64>>,
}

impl LayoutCx<'_> {
    /// Append a blueprint and return its id.
    pub fn push(&mut self, blueprint: Box<dyn NodeBlueprint>) -> NodeId {
        self.builder.push(blueprint)
    }

    /// The root rows whose morsels use a unit planned here, given the unit's own rows.
    pub fn lease_range(&self, own_rows: Range<u64>) -> Range<u64> {
        self.lease.clone().unwrap_or(own_rows)
    }

    /// Record that a fresh stored chunk starts at root row `root_row`.
    ///
    /// Ignored under a lease scope: rows of a scoped subtree are not morsel boundaries.
    pub fn split_at(&mut self, root_row: u64) {
        if self.lease.is_none() {
            self.builder.splits.push(root_row);
        }
    }

    /// Whether a child covering `root_range` must be materialized for this plan.
    ///
    /// Range-scoped plans skip chunks no planned range intersects. Scoped subtrees are always
    /// materialized.
    pub fn is_planned(&self, root_range: &Range<u64>) -> bool {
        if self.lease.is_some() {
            return true;
        }
        self.builder.planned_ranges.as_deref().is_none_or(|ranges| {
            let candidate = ranges.partition_point(|range| range.end <= root_range.start);
            ranges
                .get(candidate)
                .is_some_and(|range| range.start < root_range.end)
        })
    }

    /// Plan a child layout whose first row is at `root_offset`, inheriting this lease scope.
    pub fn child(&mut self, layout: &LayoutRef, root_offset: u64) -> VortexResult<NodeId> {
        let lease = self.lease.clone();
        self.plan_scoped(layout, root_offset, lease)
    }

    /// Plan a child layout under a lease scope of `lease` root rows.
    ///
    /// Inside an existing scope the existing scope is kept: it is the wider one, and it is the
    /// only one expressed in root rows.
    pub fn child_with_lease(
        &mut self,
        layout: &LayoutRef,
        root_offset: u64,
        lease: Range<u64>,
    ) -> VortexResult<NodeId> {
        let lease = self.lease.clone().unwrap_or(lease);
        self.plan_scoped(layout, root_offset, Some(lease))
    }

    fn plan_scoped(
        &mut self,
        layout: &LayoutRef,
        root_offset: u64,
        lease: Option<Range<u64>>,
    ) -> VortexResult<NodeId> {
        let planner = self.builder.planners.find(layout, root_offset)?;
        let mut cx = LayoutCx {
            builder: &mut *self.builder,
            lease,
        };
        planner.plan(layout, root_offset, &mut cx)
    }
}

/// A shared, immutable execution plan for one scan.
pub struct ExecPlan {
    nodes: Vec<Box<dyn NodeBlueprint>>,
    root: NodeId,
    has_predicate: bool,
    output_dtype: DType,
    row_count: u64,
    /// Root-coordinate boundaries at which every column starts a fresh chunk, used as the
    /// default morsel cut.
    natural_splits: Vec<u64>,
    /// Root-coordinate ranges for which this plan materialized layout nodes. `None` means the
    /// complete layout was planned.
    planned_ranges: Option<Arc<[Range<u64>]>>,
}

impl ExecPlan {
    /// The root node of the plan.
    pub fn root(&self) -> NodeId {
        self.root
    }

    /// The dtype the scan emits.
    pub fn output_dtype(&self) -> &DType {
        &self.output_dtype
    }

    /// The number of rows in the scanned layout.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(crate) fn has_filter(&self) -> bool {
        self.has_predicate
    }

    /// The union of every column's chunk boundaries, in root coordinates.
    pub fn natural_splits(&self) -> &[u64] {
        &self.natural_splits
    }

    /// Whether every range is covered by the layout nodes materialized in this plan.
    pub fn supports_ranges(&self, ranges: &[Range<u64>]) -> bool {
        let Some(planned) = self.planned_ranges.as_deref() else {
            return true;
        };
        ranges.iter().all(|range| {
            let idx = planned.partition_point(|candidate| candidate.end <= range.start);
            planned.get(idx).is_some_and(|candidate| {
                candidate.start <= range.start && range.end <= candidate.end
            })
        })
    }

    /// Whether this plan materialized the complete layout tree.
    pub fn is_complete(&self) -> bool {
        self.planned_ranges.is_none()
    }

    /// Every leaf's stored unit and its root-coordinate row range, one entry per node.
    ///
    /// A segment referenced from two subtrees (a column in both filter and projection) appears
    /// once per referencing node, because each node registers its own use per morsel. This is
    /// the input to the shared-cell lease counts: the count for a unit is the number of
    /// (node, morsel) pairs whose ranges overlap.
    pub fn flat_uses(&self) -> impl Iterator<Item = (IoKey, Range<u64>)> + '_ {
        self.nodes.iter().flat_map(|node| node.stored_uses())
    }

    /// How many leading morsels the initial lookahead window covers.
    ///
    /// An unfiltered dense or exact-demand scan exposes its complete read set before workers
    /// start. A filtered scan exposes two morsels per worker. Known sparse demand is left to
    /// mask-aware node planning: putting its full read set on the serial startup path delays
    /// useful CPU work.
    pub(crate) fn initial_lookahead_len(
        &self,
        morsels: &[Range<u64>],
        demands: Option<&[Mask]>,
        workers: usize,
    ) -> usize {
        debug_assert!(demands.is_none_or(|demands| demands.len() == morsels.len()));
        let sparse_demands = demands.is_some_and(|demands| {
            demands
                .iter()
                .any(|demand| !demand.all_true() && !demand.all_false())
        });
        if sparse_demands {
            0
        } else if self.has_predicate {
            morsels.len().min(workers.saturating_mul(2))
        } else {
            morsels.len()
        }
    }

    /// The stored units any of `morsels` will use, for registering lookahead reads.
    pub(crate) fn lookahead_keys(
        &self,
        morsels: &[Range<u64>],
        demands: Option<&[Mask]>,
    ) -> Vec<IoKey> {
        debug_assert!(demands.is_none_or(|demands| demands.len() == morsels.len()));
        self.flat_uses()
            .filter(|(_, range)| range_has_demand(range, morsels, demands))
            .map(|(key, _)| key)
            .collect()
    }

    /// The number of nodes in the plan.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the plan is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Instantiate one worker's mutable arena from this blueprint.
    pub fn instantiate(&self) -> Arena {
        Arena::new(
            self.nodes
                .iter()
                .enumerate()
                .map(|(idx, node)| node.instantiate(node_id(idx)))
                .collect(),
        )
    }
}

fn node_id(index: usize) -> NodeId {
    NodeId::try_from(index).vortex_expect("exec plan exceeds u32 nodes")
}

fn range_has_demand(range: &Range<u64>, morsels: &[Range<u64>], demands: Option<&[Mask]>) -> bool {
    morsels.iter().enumerate().any(|(index, morsel)| {
        let start = range.start.max(morsel.start);
        let end = range.end.min(morsel.end);
        if start >= end {
            return false;
        }
        let Some(demands) = demands else {
            return true;
        };
        let local_start = usize::try_from(start - morsel.start)
            .vortex_expect("morsel demand offset exceeds usize");
        let local_end =
            usize::try_from(end - morsel.start).vortex_expect("morsel demand offset exceeds usize");
        !demands[index].slice(local_start..local_end).all_false()
    })
}

/// Build an execution plan with the built-in planners.
///
/// See [`LayoutPlanners::build_plan`].
pub fn build_plan(
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
    mode: ConjunctMode,
) -> VortexResult<ExecPlan> {
    LayoutPlanners::default().build_plan(layout, projection, filter, mode)
}

/// Build a range-scoped plan with the built-in planners.
///
/// See [`LayoutPlanners::build_plan_for_ranges`].
pub fn build_plan_for_ranges(
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
    mode: ConjunctMode,
    ranges: &[Range<u64>],
) -> VortexResult<ExecPlan> {
    LayoutPlanners::default().build_plan_for_ranges(layout, projection, filter, mode, ranges)
}

/// Compute natural morsel ranges with the built-in planners.
///
/// See [`LayoutPlanners::natural_morsels_for`].
pub fn natural_morsels_for(
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
    target_rows: u64,
) -> VortexResult<Vec<Range<u64>>> {
    LayoutPlanners::default().natural_morsels_for(layout, projection, filter, target_rows)
}

/// The root must be a non-nullable struct layout; return its fields.
fn struct_root(layout: &LayoutRef) -> VortexResult<&StructFields> {
    let root_fields = layout
        .dtype()
        .as_struct_fields_opt()
        .ok_or_else(|| vortex_err!("the morsel executor requires a struct-rooted layout"))?;
    if !layout.is::<Struct>() {
        vortex_bail!(
            "the morsel executor requires a struct root layout, got {}",
            layout.encoding_id()
        );
    }
    Ok(root_fields)
}

/// Sort, dedupe, and bound the recorded splits, always ending at the row count.
fn finish_splits(mut splits: Vec<u64>, row_count: u64) -> Vec<u64> {
    splits.push(row_count);
    splits.sort_unstable();
    splits.dedup();
    splits.retain(|&split| split > 0 && split <= row_count);
    splits
}

fn referenced_names(
    expr: &Expression,
    dtype: &DType,
    root_fields: &StructFields,
) -> VortexResult<Vec<FieldName>> {
    let bound = expr.bind(dtype)?;
    let paths = referenced_field_paths(&bound)?;
    let mut names = Vec::new();
    for path in paths.iter() {
        if path.is_root() {
            return Ok(root_fields.names().iter().cloned().collect());
        }
        match &path.parts()[0] {
            Field::Name(name) if !names.contains(name) => names.push(name.clone()),
            Field::Name(_) => {}
            other => vortex_bail!("unsupported field reference {other:?}"),
        }
    }
    names.sort_by_key(|name| root_fields.find(name).unwrap_or(usize::MAX));
    Ok(names)
}

fn build_plan_inner(
    planners: &LayoutPlanners,
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
    mode: ConjunctMode,
    planned_ranges: Option<Arc<[Range<u64>]>>,
) -> VortexResult<ExecPlan> {
    let root_fields = struct_root(layout)?.clone();
    if layout.dtype().is_nullable() {
        vortex_bail!("the morsel executor does not support a nullable root struct");
    }

    let mut builder = Builder {
        nodes: Vec::new(),
        layout: LayoutRef::clone(layout),
        root_fields,
        splits: Vec::new(),
        planned_ranges: planned_ranges.clone(),
        planners: planners.clone(),
    };

    // The filter: one subtree per conjunct, each over just that conjunct's fields.
    let predicate = match filter {
        None => None,
        Some(filter) => {
            let conjuncts = split_conjuncts(filter);
            let mut slots = Vec::with_capacity(conjuncts.len());
            for conjunct in conjuncts {
                let (input, bound) = builder.build_scoped(&conjunct)?;
                slots.push((input, bound));
            }
            Some(builder.push(Box::new(ConjunctSpec { slots, mode })))
        }
    };

    // The projection.
    let (projection_input, projection_bound) = builder.build_scoped(projection)?;
    let output_dtype = projection_bound.dtype().clone();
    let root = builder.push(Box::new(FilterSpec {
        predicate,
        projection: projection_input,
        expr: projection_bound,
        dtype: output_dtype.clone(),
    }));

    let row_count = layout.row_count();
    Ok(ExecPlan {
        nodes: builder.nodes,
        root,
        has_predicate: predicate.is_some(),
        output_dtype,
        row_count,
        natural_splits: finish_splits(builder.splits, row_count),
        planned_ranges,
    })
}

struct Builder {
    nodes: Vec<Box<dyn NodeBlueprint>>,
    layout: LayoutRef,
    root_fields: StructFields,
    splits: Vec<u64>,
    planned_ranges: Option<Arc<[Range<u64>]>>,
    planners: LayoutPlanners,
}

impl Builder {
    fn push(&mut self, blueprint: Box<dyn NodeBlueprint>) -> NodeId {
        self.nodes.push(blueprint);
        node_id(self.nodes.len() - 1)
    }

    /// Build the subtree for one expression: a struct over exactly the top-level fields the
    /// expression reads, plus that expression re-bound against the narrowed struct dtype.
    fn build_scoped(&mut self, expr: &Expression) -> VortexResult<(NodeId, BoundExpression)> {
        let full = expr.bind(self.layout.dtype())?;
        let names = self.referenced_top_level_fields(&full)?;

        let dtypes = names
            .iter()
            .map(|name| {
                self.root_fields
                    .field(name)
                    .ok_or_else(|| vortex_err!("field {name} not found in the scan dtype"))
            })
            .collect::<VortexResult<Vec<_>>>()?;
        let narrowed = DType::Struct(
            StructFields::new(FieldNames::from(names.clone()), dtypes),
            Nullability::NonNullable,
        );
        let bound = expr.bind(&narrowed)?;

        let mut children = Vec::with_capacity(names.len());
        for name in &names {
            let idx = self
                .root_fields
                .find(name)
                .ok_or_else(|| vortex_err!("field {name} not found in the scan dtype"))?;
            let field_layout = self
                .layout
                .slot(idx + 1)?
                .ok_or_else(|| vortex_err!("struct layout has no child for field {idx}"))?;
            let mut cx = LayoutCx {
                builder: self,
                lease: None,
            };
            children.push(cx.child(&field_layout, 0)?);
        }

        let node = self.push(Box::new(StructSpec {
            names: FieldNames::from(names),
            children: Arc::from(children),
            validity: None,
        }));
        Ok((node, bound))
    }

    fn referenced_top_level_fields(&self, expr: &BoundExpression) -> VortexResult<Vec<FieldName>> {
        let paths = referenced_field_paths(expr)?;
        let mut names: Vec<FieldName> = Vec::new();
        let mut all = false;
        for path in paths.iter() {
            if path.is_root() {
                all = true;
                break;
            }
            match &path.parts()[0] {
                Field::Name(name) => {
                    if !names.contains(name) {
                        names.push(name.clone());
                    }
                }
                other => vortex_bail!("unsupported field reference {other:?}"),
            }
        }
        if all {
            names = self.root_fields.names().iter().cloned().collect();
        }
        // Keep the scan dtype's field order so `select` and `pack` see the fields they expect.
        names.sort_by_key(|name| self.root_fields.find(name).unwrap_or(usize::MAX));
        Ok(names)
    }
}

/// Split a conjunction into its conjuncts, mirroring the V1 `FilterExpr` split.
fn split_conjuncts(expr: &Expression) -> Vec<Expression> {
    use vortex_array::scalar_fn::fns::binary::Binary;
    use vortex_array::scalar_fn::fns::operators::Operator;

    let mut conjuncts = Vec::new();
    let mut pending = vec![expr.clone()];
    while let Some(expr) = pending.pop() {
        let is_and = expr
            .as_scalar()
            .and_then(|scalar_fn| scalar_fn.as_opt::<Binary>())
            .is_some_and(|operator| *operator == Operator::And);
        if is_and {
            pending.extend(expr.children().iter().rev().cloned());
        } else {
            conjuncts.push(expr);
        }
    }
    conjuncts
}

/// The morsel row ranges for a plan, from its natural splits, coalesced to `target_rows`.
pub(crate) fn cut_morsels(splits: &[u64], target_rows: u64) -> Vec<Range<u64>> {
    let mut morsels = Vec::new();
    let mut start = 0u64;
    for &split in splits {
        if split <= start {
            continue;
        }
        if split - start >= target_rows {
            morsels.push(start..split);
            start = split;
        }
    }
    if let Some(&last) = splits.last()
        && last > start
    {
        morsels.push(start..last);
    }
    morsels
}
