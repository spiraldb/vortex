// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Building an executable plan from a physical plan.
//!
//! A scan is one [`PlanRef`] tree from [`vortex_layout::plan`]: an [`Eval`] of the projection
//! over a [`Filter`] of the scanned fields by a boolean mask plan, or over the fields alone when
//! there is no predicate. [`build_plan`] derives that tree from a layout by lowering it and
//! scoping each expression to the top-level fields it reads; [`ExecPlan::from_plan`] accepts a
//! tree built or optimized elsewhere.
//!
//! There is no intermediate representation. An [`ExecPlan`] is the plan itself plus the few
//! facts the driver needs before any worker runs: the natural splits that cut morsels, the
//! stored units each subtree reads and the root rows that lease them, and a couple of counts.
//! [`ExecPlan::instantiate`] builds a worker's operator tree straight from the plan, carrying
//! the root offset, lease scope, and filter placement down the same recursion.
//!
//! The filter is pushed to the leaves here. Every segment scan under the filter's input gets its
//! own filter operator, and all of them read one mask buffer that the root fills from the mask
//! producer it owns, which produces the morsel's selection once.

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
use vortex_array::expr::root;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_layout::LayoutRef;
use vortex_layout::layouts::struct_::Struct;
use vortex_layout::plan::Concat;
use vortex_layout::plan::ConcatPlan;
use vortex_layout::plan::Eval;
use vortex_layout::plan::EvalPlan;
use vortex_layout::plan::ExactPlan;
use vortex_layout::plan::Filter;
use vortex_layout::plan::FilterPlan;
use vortex_layout::plan::Pack;
use vortex_layout::plan::PackPlan;
use vortex_layout::plan::PlanRef;
use vortex_layout::plan::SegmentScan;
use vortex_layout::plan::Take;
use vortex_layout::plan::lower;
use vortex_mask::Mask;

use crate::demand::RowDomain;
use crate::io::IoKey;
use crate::io::ProducerId;
use crate::node::Child;
use crate::node::NodeId;
use crate::node::Operator;
use crate::node::Tree;
use crate::nodes::ChunkedExec;
use crate::nodes::ConjunctExec;
use crate::nodes::ConjunctSlot;
use crate::nodes::DemandExec;
use crate::nodes::DictExec;
use crate::nodes::EvalExec;
use crate::nodes::FilterExec;
use crate::nodes::FlatExec;
use crate::nodes::ProjectExec;
use crate::nodes::StructExec;

/// The one mask buffer a scan has: every filter under the body reads it.
const MASK_TEE: usize = 0;

/// A shared, immutable execution plan for one scan.
pub struct ExecPlan {
    /// The projection applied at the root.
    projection: BoundExpression,
    /// The filter's conjuncts, each with the plan producing its input, in evaluation order.
    /// `None` when the scan has no predicate.
    conjuncts: Option<Vec<(PlanRef, BoundExpression)>>,
    /// The scanned fields the projection reads.
    body: PlanRef,
    output_dtype: DType,
    row_count: u64,
    /// Root-coordinate boundaries at which every column starts a fresh chunk, used as the
    /// default morsel cut.
    natural_splits: Vec<u64>,
    /// Every stored unit and the root rows whose morsels use it, one entry per reading leaf.
    uses: Vec<(IoKey, Range<u64>)>,
    /// Operators in the complete plan, for tracing.
    node_count: usize,
    /// Pre-order position after each subtree, for skipping unneeded chunks with stable trace IDs.
    subtree_ends: Vec<NodeId>,
    /// Root-coordinate ranges for which this plan materializes chunks. `None` means every chunk.
    planned_ranges: Option<Arc<[Range<u64>]>>,
}

impl ExecPlan {
    /// Build an executable plan from a scan's plan.
    ///
    /// The root is an [`Eval`] of the projection, or a bare plan under the identity. Its child
    /// is either a [`Filter`] of the scanned fields by a boolean mask plan, or the fields alone.
    /// An [`Eval`] over a non-nullable [`Pack`] as the mask is split into its conjuncts, each
    /// scoped to the fields it reads and evaluated in cascade; any other boolean plan is one
    /// conjunct evaluated whole. Every segment scan under the fields gets a filter reading the
    /// morsel's mask.
    pub fn from_plan(plan: PlanRef) -> VortexResult<Self> {
        Self::lower_scan(plan, None)
    }

    /// Like [`Self::from_plan`], materializing only the chunks that `ranges` reach.
    ///
    /// The ranges must be sorted, non-empty and non-overlapping. The plan still records every
    /// natural split, but a morsel that reaches an unmaterialized chunk fails when it is opened.
    pub fn from_plan_for_ranges(plan: PlanRef, ranges: &[Range<u64>]) -> VortexResult<Self> {
        validate_ranges(ranges, plan.row_count())?;
        Self::lower_scan(plan, Some(Arc::from(ranges)))
    }

    fn lower_scan(plan: PlanRef, planned_ranges: Option<Arc<[Range<u64>]>>) -> VortexResult<Self> {
        let row_count = plan.row_count();
        let (projection, body) = match plan.as_opt::<Eval>() {
            Some(eval) => (eval.expression().clone(), eval.child_plan()?),
            None => (root().bind(plan.dtype())?, plan.clone()),
        };
        let output_dtype = projection.dtype().clone();
        let (conjuncts, body) = match body.as_opt::<Filter>() {
            Some(filter) => (Some(mask_slots(&filter.mask()?)?), filter.input()?),
            None => (None, body),
        };

        // One walk over everything a tree will contain, in the order a tree is built, to
        // collect what the driver needs before any worker instantiates one.
        let mut survey = Survey {
            splits: Vec::new(),
            uses: Vec::new(),
            // The root and its mask producer.
            nodes: 2,
            subtree_ends: Vec::new(),
            planned_ranges: planned_ranges.as_deref(),
        };
        if let Some(conjuncts) = &conjuncts {
            for (input, _) in conjuncts {
                survey.visit(input, 0, None, false)?;
            }
        }
        survey.visit(&body, 0, None, true)?;

        Ok(ExecPlan {
            projection,
            conjuncts,
            body,
            output_dtype,
            row_count,
            natural_splits: finish_splits(survey.splits, row_count),
            uses: survey.uses,
            node_count: survey.nodes,
            subtree_ends: survey.subtree_ends,
            planned_ranges,
        })
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
        self.conjuncts.is_some()
    }

    /// The union of every column's chunk boundaries, in root coordinates.
    pub fn natural_splits(&self) -> &[u64] {
        &self.natural_splits
    }

    /// Whether every range is covered by the chunks materialized in this plan.
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

    /// Whether this plan materialized every chunk.
    pub fn is_complete(&self) -> bool {
        self.planned_ranges.is_none()
    }

    /// Every stored unit and the root rows whose morsels use it, one entry per reading leaf.
    ///
    /// A segment referenced from two subtrees (a column in both filter and projection) appears
    /// once per leaf, because each leaf registers its own use per morsel. This is the input to
    /// the shared-cell lease counts: the count for a unit is the number of (leaf, morsel) pairs
    /// whose ranges overlap.
    pub fn flat_uses(&self) -> impl Iterator<Item = (IoKey, Range<u64>)> + '_ {
        self.uses.iter().cloned()
    }

    /// How many leading morsels the initial lookahead window covers.
    ///
    /// An unfiltered dense or exact-demand scan exposes its complete read set before workers
    /// start. A filtered scan exposes two morsels per worker. Known sparse demand is left to
    /// mask-aware planning: putting its full read set on the serial startup path delays useful
    /// CPU work.
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
        } else if self.has_filter() {
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

    /// The number of operators in the complete plan.
    pub fn len(&self) -> usize {
        self.node_count
    }

    /// Whether the plan is empty.
    pub fn is_empty(&self) -> bool {
        self.node_count == 0
    }

    /// Instantiate the operators needed by one morsel, on the thread that will drive it.
    ///
    /// `range` must be non-empty and covered by this plan. The tree is closed and dropped when
    /// the morsel finishes; it cannot be reused for another range.
    pub fn instantiate(&self, range: Range<u64>) -> Tree {
        let rows = usize::try_from(range.end - range.start).vortex_expect("morsel rows fit usize");
        self.instantiate_with_demand(range, Mask::new_true(rows))
    }

    /// Instantiate a morsel with its initial row selection.
    pub fn instantiate_with_demand(&self, range: Range<u64>, demand: Mask) -> Tree {
        debug_assert!(!range.is_empty() && range.end <= self.row_count);
        debug_assert!(self.supports_ranges(std::slice::from_ref(&range)));
        let mut builder = Builder {
            plan: self,
            next_id: 0,
        };
        // Lowering already forced every plan child and rejected every unsupported operator, and
        // plan children are memoized, so the same walk cannot fail here.
        let root = builder
            .root(RowDomain::new(range.clone(), demand).vortex_expect("valid morsel demand"))
            .vortex_expect("the plan was validated when it was lowered");
        debug_assert_eq!(builder.next_id as usize, self.node_count);
        Tree::new(root, 1, range.start)
    }
}

/// Whether a chunk covering `root_range` must be materialized.
fn is_planned(planned_ranges: Option<&[Range<u64>]>, root_range: &Range<u64>) -> bool {
    planned_ranges.is_none_or(|ranges| {
        let candidate = ranges.partition_point(|range| range.end <= root_range.start);
        ranges
            .get(candidate)
            .is_some_and(|range| range.start < root_range.end)
    })
}

/// Cumulative chunk offsets of a [`Concat`], ending at its row count.
fn chunk_offsets(plan: &PlanRef, concat: &ConcatPlan) -> VortexResult<Vec<u64>> {
    let nchunks = plan.child_count();
    if concat.row_offsets().len() != nchunks {
        vortex_bail!(
            "Concat has {} row offsets for {nchunks} children",
            concat.row_offsets().len()
        );
    }
    let mut offsets = Vec::with_capacity(nchunks + 1);
    offsets.extend_from_slice(concat.row_offsets());
    offsets.push(plan.row_count());
    Ok(offsets)
}

/// The conjuncts of a mask plan, each with the plan producing its input.
///
/// An [`Eval`] over a non-nullable [`Pack`] is split into its conjuncts, each over a `Pack` of
/// only the fields it reads, so a conjunct never touches a column another conjunct needs. Any
/// other boolean plan is a single conjunct evaluated whole.
fn mask_slots(mask: &PlanRef) -> VortexResult<Vec<(PlanRef, BoundExpression)>> {
    if !mask.dtype().is_boolean() {
        vortex_bail!(
            "a filter mask plan must produce booleans, got {}",
            mask.dtype()
        );
    }
    if let Some(eval) = mask.as_opt::<Eval>()
        && let input = eval.child_plan()?
        && let Some(pack) = input.as_opt::<Pack>()
        && !pack.dtype().is_nullable()
    {
        let fields = pack.fields().clone();
        let mut slots = Vec::new();
        for conjunct in split_conjuncts(&unbind(eval.expression())?) {
            let scoped = scoped_eval(pack, &fields, input.dtype(), &conjunct)?;
            let scoped = scoped.as_::<Eval>();
            slots.push((scoped.child_plan()?, scoped.expression().clone()));
        }
        return Ok(slots);
    }
    Ok(vec![(mask.clone(), root().bind(mask.dtype())?)])
}

/// The walk over a plan that collects what the driver needs, in the order a tree is built.
///
/// A walk carries a *lease scope*: the root rows whose morsels use whatever is under it.
/// Normally a stored unit is used by the morsels covering its own rows, but a dictionary's
/// values are used by every morsel of the codes' range, so the values subtree of a [`Take`] is
/// scoped to that range. Scoped subtrees record no natural splits, are never pruned by the
/// planned ranges, and are never filtered. Scopes only widen: a scope opened inside another
/// keeps the outer one, because the outer range is already in root rows while an inner
/// operator's own rows are not.
struct Survey<'a> {
    splits: Vec<u64>,
    uses: Vec<(IoKey, Range<u64>)>,
    nodes: usize,
    subtree_ends: Vec<NodeId>,
    planned_ranges: Option<&'a [Range<u64>]>,
}

impl Survey<'_> {
    fn visit(
        &mut self,
        plan: &PlanRef,
        root_offset: u64,
        lease: Option<Range<u64>>,
        filtered: bool,
    ) -> VortexResult<()> {
        let own_rows = root_offset..root_offset + plan.row_count();
        let start = self.nodes;
        self.nodes += 1;
        if let Some(scan) = plan.as_opt::<SegmentScan>() {
            if lease.is_none() {
                self.splits.push(own_rows.end);
            }
            self.uses
                .push((IoKey::Segment(scan.segment_id()), lease.unwrap_or(own_rows)));
            // The filter placed over the leaf.
            self.nodes += usize::from(filtered);
        } else if let Some(concat) = plan.as_opt::<Concat>() {
            let offsets = chunk_offsets(plan, concat)?;
            for chunk in 0..plan.child_count() {
                let chunk_range = root_offset + offsets[chunk]..root_offset + offsets[chunk + 1];
                if lease.is_none() {
                    self.splits.push(chunk_range.end);
                    if !is_planned(self.planned_ranges, &chunk_range) {
                        continue;
                    }
                }
                let child = plan.child_required(chunk)?;
                self.visit(&child, chunk_range.start, lease.clone(), filtered)?;
            }
        } else if plan.is::<Pack>() {
            for index in 0..plan.child_count() {
                let child = plan.child_required(index)?;
                self.visit(&child, root_offset, lease.clone(), filtered)?;
            }
        } else if let Some(take) = plan.as_opt::<Take>() {
            // The codes are in the row domain and take the filter; the values are not.
            let values_lease = lease.clone().unwrap_or(own_rows);
            self.visit(&take.codes()?, root_offset, lease, filtered)?;
            self.visit(&take.values()?, root_offset, Some(values_lease), false)?;
        } else if let Some(eval) = plan.as_opt::<Eval>() {
            self.visit(&eval.child_plan()?, root_offset, lease, filtered)?;
        } else {
            vortex_bail!(
                "the morsel executor has no operator for plan {} at row offset {root_offset}",
                plan.id()
            );
        }
        self.subtree_ends.resize(self.nodes, 0);
        self.subtree_ends[start] = self
            .nodes
            .try_into()
            .vortex_expect("operator count fits NodeId");
        Ok(())
    }
}

/// Builds one morsel's tree from the plan, in the same order [`Survey`] walked it.
///
/// Trace ids are assigned in pre-order, so every worker names the same operator the same way.
struct Builder<'a> {
    plan: &'a ExecPlan,
    next_id: NodeId,
}

impl Builder<'_> {
    fn skip(&mut self) {
        self.next_id = self.plan.subtree_ends[self.next_id as usize];
    }

    fn id(&mut self) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn root(&mut self, domain: RowDomain) -> VortexResult<Child> {
        let plan = self.plan;
        let id = self.id();
        let mask = match &plan.conjuncts {
            Some(conjuncts) => {
                let mask_id = self.id();
                let slots = conjuncts
                    .iter()
                    .map(|(input, predicate)| {
                        Ok(ConjunctSlot {
                            input: self.build(
                                input,
                                0,
                                None,
                                None,
                                RowDomain::new(domain.range().clone(), domain.snapshot())?,
                            )?,
                            predicate: predicate.clone(),
                        })
                    })
                    .collect::<VortexResult<Vec<_>>>()?;
                Child::new(mask_id, Box::new(ConjunctExec::new(slots, domain.clone())))
            }
            None => {
                let mask_id = self.id();
                Child::new(mask_id, Box::new(DemandExec::new(domain.clone())))
            }
        };
        let body = self.build(
            &plan.body,
            0,
            None,
            Some(MASK_TEE),
            RowDomain::new(domain.range().clone(), domain.snapshot())?,
        )?;
        Ok(Child::new(
            id,
            Box::new(ProjectExec::new(
                mask,
                body,
                MASK_TEE,
                plan.conjuncts.is_some(),
                plan.projection.clone(),
                plan.output_dtype.clone(),
                domain,
            )),
        ))
    }

    /// Build the operator for `plan` at `root_offset`, under `lease` and, when `filter` names a
    /// mask buffer, with a filter over every leaf.
    fn build(
        &mut self,
        plan: &PlanRef,
        root_offset: u64,
        lease: Option<Range<u64>>,
        filter: Option<usize>,
        domain: RowDomain,
    ) -> VortexResult<Child> {
        let own_rows = root_offset..root_offset + plan.row_count();
        if let Some(scan) = plan.as_opt::<SegmentScan>() {
            let filter_id = filter.map(|_| self.id());
            let leaf_id = self.id();
            let leaf = Child::new(
                leaf_id,
                Box::new(FlatExec::new(
                    scan,
                    root_offset,
                    ProducerId(leaf_id),
                    lease.unwrap_or(own_rows),
                    domain.clone(),
                )),
            );
            return Ok(match (filter, filter_id) {
                (Some(tee), Some(id)) => Child::new(
                    id,
                    Box::new(FilterExec::new(
                        leaf,
                        tee,
                        root_offset,
                        plan.dtype().clone(),
                        domain,
                    )),
                ),
                _ => leaf,
            });
        }

        let id = self.id();
        let op: Box<dyn Operator> = if let Some(concat) = plan.as_opt::<Concat>() {
            let offsets = chunk_offsets(plan, concat)?;
            let mut children = Vec::with_capacity(plan.child_count());
            for chunk in 0..plan.child_count() {
                let chunk_range = root_offset + offsets[chunk]..root_offset + offsets[chunk + 1];
                if lease.is_none() && !is_planned(self.plan.planned_ranges.as_deref(), &chunk_range)
                {
                    children.push(None);
                    continue;
                }
                let start = domain.range().start.max(offsets[chunk]);
                let end = domain.range().end.min(offsets[chunk + 1]);
                if start >= end {
                    self.skip();
                    children.push(None);
                    continue;
                }
                let child = plan.child_required(chunk)?;
                children.push(Some(self.build(
                    &child,
                    chunk_range.start,
                    lease.clone(),
                    filter,
                    domain.slice(start..end)?.rebase(start - offsets[chunk])?,
                )?));
            }
            Box::new(ChunkedExec::new(
                Arc::from(offsets),
                children,
                plan.dtype().clone(),
                domain,
            )?)
        } else if let Some(pack) = plan.as_opt::<Pack>() {
            // Fields first, then the validity child when the struct is nullable.
            let nfields = pack.nfields();
            let mut children = Vec::with_capacity(plan.child_count());
            for index in 0..plan.child_count() {
                let child = plan.child_required(index)?;
                children.push(self.build(
                    &child,
                    root_offset,
                    lease.clone(),
                    filter,
                    domain.clone(),
                )?);
            }
            let validity = (children.len() > nfields).then(|| children.pop()).flatten();
            Box::new(StructExec::new(
                pack.fields().names().clone(),
                children,
                validity,
                domain,
            ))
        } else if let Some(take) = plan.as_opt::<Take>() {
            let values = take.values()?;
            let values_key = ExactPlan(values.clone());
            let values_len = usize::try_from(values.row_count())
                .map_err(|_| vortex_err!("dictionary values row count exceeds usize"))?;
            let values_lease = lease.clone().unwrap_or(own_rows);
            let codes = self.build(&take.codes()?, root_offset, lease, filter, domain.clone())?;
            let values = self.build(
                &values,
                root_offset,
                Some(values_lease),
                None,
                RowDomain::new(0..values_len as u64, Mask::new_true(values_len))?,
            )?;
            Box::new(DictExec::new(values_key, values, codes, values_len, domain))
        } else if let Some(eval) = plan.as_opt::<Eval>() {
            let child = self.build(
                &eval.child_plan()?,
                root_offset,
                lease,
                filter,
                domain.clone(),
            )?;
            Box::new(EvalExec::new(
                child,
                eval.expression().clone(),
                plan.dtype().clone(),
                domain,
            ))
        } else {
            vortex_bail!(
                "the morsel executor has no operator for plan {} at row offset {root_offset}",
                plan.id()
            );
        };
        Ok(Child::new(id, op))
    }
}

/// Build an execution plan for `projection` filtered by `filter` over `layout`.
///
/// The layout is lowered to its physical plan; the projection and the filter each become an
/// [`Eval`] over a [`Pack`] of just the top-level fields they read, and the filter's `Eval` is
/// the mask of a [`Filter`] over the projection's fields. Mirrors the V1 scan's expression
/// split so the two executors read the same columns for the same query.
pub fn build_plan(
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
) -> VortexResult<ExecPlan> {
    ExecPlan::from_plan(scan_plan(layout, projection, filter)?)
}

/// Like [`build_plan`], materializing only the chunks that `ranges` reach.
///
/// See [`ExecPlan::from_plan_for_ranges`].
pub fn build_plan_for_ranges(
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
    ranges: &[Range<u64>],
) -> VortexResult<ExecPlan> {
    ExecPlan::from_plan_for_ranges(scan_plan(layout, projection, filter)?, ranges)
}

/// The natural morsel cut for a query: every row at which one of the columns it reads starts a
/// fresh chunk, coalesced to `target_rows`.
pub fn natural_morsels_for(
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
    target_rows: u64,
) -> VortexResult<Vec<Range<u64>>> {
    let plan = build_plan(layout, projection, filter)?;
    Ok(cut_morsels(plan.natural_splits(), target_rows))
}

/// The scan's plan: `Eval(projection, Filter(fields, Eval(filter, fields)))` over `layout`,
/// with each `Eval` scoped to the top-level fields its expression reads.
pub fn scan_plan(
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
) -> VortexResult<PlanRef> {
    let root_fields = struct_root(layout)?.clone();
    if layout.dtype().is_nullable() {
        vortex_bail!("the morsel executor does not support a nullable root struct");
    }
    let lowered = lower(layout)?;
    let pack = lowered
        .as_opt::<Pack>()
        .ok_or_else(|| vortex_err!("a struct layout must lower to a Pack, got {}", lowered.id()))?;
    let projection = scoped_eval(pack, &root_fields, layout.dtype(), projection)?;
    let Some(filter) = filter else {
        return Ok(projection);
    };
    let mask = scoped_eval(pack, &root_fields, layout.dtype(), filter)?;
    let projection = projection.as_::<Eval>();
    let filtered = FilterPlan::try_new(projection.child_plan()?, mask)?.into_plan();
    Ok(EvalPlan::try_new(projection.expression().clone(), filtered)?.into_plan())
}

/// An [`Eval`] of `expr` over a [`Pack`] of exactly the top-level fields it reads, with the
/// expression re-bound against that narrowed struct.
fn scoped_eval(
    pack: &PackPlan,
    root_fields: &StructFields,
    dtype: &DType,
    expr: &Expression,
) -> VortexResult<PlanRef> {
    let names = referenced_names(expr, dtype, root_fields)?;
    let mut dtypes = Vec::with_capacity(names.len());
    let mut fields = Vec::with_capacity(names.len());
    for name in &names {
        let index = root_fields
            .find(name)
            .ok_or_else(|| vortex_err!("field {name} not found in the scan dtype"))?;
        dtypes.push(
            root_fields
                .field(name)
                .ok_or_else(|| vortex_err!("field {name} not found in the scan dtype"))?,
        );
        fields.push(pack.child_required(index)?);
    }
    let narrowed = StructFields::new(FieldNames::from(names), dtypes);
    let bound = expr.bind(&DType::Struct(narrowed.clone(), Nullability::NonNullable))?;
    let input = PackPlan::try_new(
        narrowed,
        Nullability::NonNullable,
        pack.row_count(),
        fields,
        None,
    )?;
    Ok(EvalPlan::try_new(bound, input.into_plan())?.into_plan())
}

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

fn validate_ranges(ranges: &[Range<u64>], row_count: u64) -> VortexResult<()> {
    if ranges.is_empty() {
        vortex_bail!("a range-scoped morsel plan requires at least one range");
    }
    let mut previous_end = 0;
    for (idx, range) in ranges.iter().enumerate() {
        if range.start >= range.end {
            vortex_bail!("planned range must be non-empty, got {range:?}");
        }
        if range.end > row_count {
            vortex_bail!("planned range {range:?} exceeds the plan row count {row_count}");
        }
        if idx > 0 && range.start < previous_end {
            vortex_bail!("planned ranges must be sorted and non-overlapping");
        }
        previous_end = range.end;
    }
    Ok(())
}

/// Sort, dedupe, and bound the recorded splits, always ending at the row count.
fn finish_splits(mut splits: Vec<u64>, row_count: u64) -> Vec<u64> {
    splits.push(row_count);
    splits.sort_unstable();
    splits.dedup();
    splits.retain(|&split| split > 0 && split <= row_count);
    splits
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

/// The top-level fields `expr` reads, in the scan dtype's field order.
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

/// Recover the unbound expression from a bound one.
pub(crate) fn unbind(expr: &BoundExpression) -> VortexResult<Expression> {
    let Some(scalar_fn) = expr.as_scalar() else {
        return Ok(Expression::Root);
    };
    Expression::try_new(
        scalar_fn.clone(),
        expr.children()
            .iter()
            .map(unbind)
            .collect::<VortexResult<Vec<_>>>()?,
    )
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
