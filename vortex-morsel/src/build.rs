// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Building an [`ExecPlan`] from a layout tree and a query.
//!
//! The plan is the immutable half of the design's split: one blueprint per scan. Each worker
//! instantiates one thread-local [`Arena`], whose node state survives IO suspension and is recycled
//! across that worker's morsels without crossing a thread boundary.
//!
//! Only the layouts and expression shapes named in the P1 scope are accepted. Anything else is a
//! build error rather than a silent fallback, so an unsupported query can never be timed as if
//! the prototype had executed it.

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
use vortex_layout::layouts::chunked::Chunked;
use vortex_layout::layouts::dict::Dict;
use vortex_layout::layouts::flat::Flat;
use vortex_layout::layouts::flat::FlatLayout;
use vortex_layout::layouts::struct_::Struct;
use vortex_layout::layouts::zoned::LegacyStats;
use vortex_layout::layouts::zoned::Zoned;
use vortex_mask::Mask;

use crate::io::IoKey;
use crate::io::ProducerId;
use crate::node::Arena;
use crate::node::ExecNode;
use crate::node::NodeId;
use crate::nodes::ChunkedExec;
use crate::nodes::ConjunctExec;
use crate::nodes::ConjunctMode;
use crate::nodes::ConjunctSlot;
use crate::nodes::DictExec;
use crate::nodes::FilterExec;
use crate::nodes::FlatExec;
use crate::nodes::StructExec;

/// The immutable blueprint of one node.
enum NodeSpec {
    Flat {
        layout: FlatLayout,
        root_offset: u64,
        lease_range: Range<u64>,
    },
    Chunked {
        chunk_offsets: Arc<[u64]>,
        child_chunks: Arc<[usize]>,
        children: Arc<[NodeId]>,
        dtype: DType,
    },
    Struct {
        names: FieldNames,
        children: Arc<[NodeId]>,
        validity: Option<NodeId>,
    },
    Dict {
        values: NodeId,
        codes: NodeId,
        values_len: usize,
    },
    Conjunct {
        slots: Vec<(NodeId, BoundExpression)>,
        mode: ConjunctMode,
    },
    Filter {
        predicate: Option<NodeId>,
        projection: NodeId,
        expr: BoundExpression,
        dtype: DType,
    },
}

/// A shared, immutable execution plan for one scan.
pub struct ExecPlan {
    nodes: Vec<NodeSpec>,
    root: NodeId,
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
        matches!(
            &self.nodes[self.root as usize],
            NodeSpec::Filter {
                predicate: Some(_),
                ..
            }
        )
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

    /// Every flat node's stored unit and its root-coordinate row range, one entry per node.
    ///
    /// A segment referenced from two subtrees (a column in both filter and projection) appears
    /// once per referencing node, because each node registers its own use per morsel. This is
    /// the input to the shared-cell lease counts: the count for a unit is the number of
    /// (node, morsel) pairs whose ranges overlap.
    pub fn flat_uses(&self) -> impl Iterator<Item = (IoKey, Range<u64>)> + '_ {
        self.nodes.iter().filter_map(|spec| match spec {
            NodeSpec::Flat {
                layout,
                lease_range,
                ..
            } => Some((IoKey::Segment(layout.segment_id()), lease_range.clone())),
            _ => None,
        })
    }

    /// Stored units to expose to the shared I/O service before workers start.
    ///
    /// An unfiltered dense or exact-demand scan can expose its complete exact set. A filtered scan
    /// exposes two morsels per worker. Known sparse demand is left to mask-aware node planning:
    /// putting its full read set on the serial startup path delays useful CPU work.
    /// How many leading morsels the initial lookahead window covers.
    pub(crate) fn initial_lookahead_len(
        &self,
        morsels: &[Range<u64>],
        demands: Option<&[Mask]>,
        workers: usize,
    ) -> usize {
        debug_assert!(demands.is_none_or(|demands| demands.len() == morsels.len()));
        let NodeSpec::Filter { predicate, .. } = &self.nodes[self.root as usize] else {
            unreachable!("the plan root is always a filter node")
        };
        let sparse_demands = demands.is_some_and(|demands| {
            demands
                .iter()
                .any(|demand| !demand.all_true() && !demand.all_false())
        });
        if sparse_demands {
            0
        } else if predicate.is_some() {
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
        let nodes: Vec<Box<dyn ExecNode>> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(idx, spec)| -> Box<dyn ExecNode> {
                match spec {
                    NodeSpec::Flat {
                        layout,
                        root_offset,
                        lease_range,
                    } => Box::new(FlatExec::new(
                        layout,
                        *root_offset,
                        lease_range.clone(),
                        ProducerId(u32::try_from(idx).unwrap_or(u32::MAX)),
                    )),
                    NodeSpec::Chunked {
                        chunk_offsets,
                        child_chunks,
                        children,
                        dtype,
                    } => Box::new(ChunkedExec::new(
                        Arc::clone(chunk_offsets),
                        Arc::clone(child_chunks),
                        Arc::clone(children),
                        dtype.clone(),
                    )),
                    NodeSpec::Struct {
                        names,
                        children,
                        validity,
                    } => Box::new(StructExec::new(
                        names.clone(),
                        Arc::clone(children),
                        *validity,
                    )),
                    NodeSpec::Dict {
                        values,
                        codes,
                        values_len,
                    } => Box::new(DictExec::new(
                        u32::try_from(idx).unwrap_or(u32::MAX),
                        *values,
                        *codes,
                        *values_len,
                    )),
                    NodeSpec::Conjunct { slots, mode } => Box::new(ConjunctExec::new(
                        slots
                            .iter()
                            .map(|(input, predicate)| ConjunctSlot {
                                input: *input,
                                predicate: predicate.clone(),
                            })
                            .collect(),
                        *mode,
                    )),
                    NodeSpec::Filter {
                        predicate,
                        projection,
                        expr,
                        dtype,
                    } => Box::new(FilterExec::new(
                        *predicate,
                        *projection,
                        expr.clone(),
                        dtype.clone(),
                    )),
                }
            })
            .collect();
        Arena::new(nodes)
    }
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

/// Build an execution plan for `layout` under `projection` and `filter`.
///
/// The expressions are *unbound*: each conjunct and the projection are re-bound against the
/// narrowed struct dtype of just the fields they reference, which is what lets a subtree read
/// only its own columns without any expression rewriting.
pub fn build_plan(
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
    mode: ConjunctMode,
) -> VortexResult<ExecPlan> {
    build_plan_inner(layout, projection, filter, mode, None)
}

/// Build a plan that materializes only layout chunks intersecting `ranges`.
///
/// The ranges use root row coordinates and must be sorted, non-overlapping, non-empty, and within
/// the layout row count. The resulting plan rejects scans outside those ranges.
pub fn build_plan_for_ranges(
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
    build_plan_inner(layout, projection, filter, mode, Some(Arc::from(ranges)))
}

/// Compute natural morsel ranges for the referenced columns without materializing indivisible
/// chunk children.
///
/// This mirrors the lazy V1 split walk: chunk row counts and indivisibility come from serialized
/// child metadata, so an all-flat chunked column contributes its boundaries without constructing
/// every child layout.
pub fn natural_morsels_for(
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
    target_rows: u64,
) -> VortexResult<Vec<Range<u64>>> {
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
    for name in names {
        let idx = root_fields
            .find(&name)
            .ok_or_else(|| vortex_err!("field {name} not found in the scan dtype"))?;
        let field = layout
            .slot(idx + 1)?
            .ok_or_else(|| vortex_err!("struct layout has no child for field {idx}"))?;
        collect_lazy_splits(&field, 0, &mut splits)?;
    }
    splits.push(layout.row_count());
    splits.sort_unstable();
    splits.dedup();
    splits.retain(|&split| split > 0 && split <= layout.row_count());
    Ok(cut_morsels(&splits, target_rows))
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

fn collect_lazy_splits(
    layout: &LayoutRef,
    root_offset: u64,
    splits: &mut Vec<u64>,
) -> VortexResult<()> {
    if layout.is::<Zoned>() || layout.is::<LegacyStats>() {
        let data = layout
            .slot(0)?
            .ok_or_else(|| vortex_err!("zoned layout has no data child"))?;
        return collect_lazy_splits(&data, root_offset, splits);
    }
    if layout.is::<Flat>() {
        splits.push(root_offset + layout.row_count());
        return Ok(());
    }
    if layout.is::<Dict>() {
        let codes = layout
            .slot(1)?
            .ok_or_else(|| vortex_err!("dictionary layout has no codes child"))?;
        return collect_lazy_splits(&codes, root_offset, splits);
    }
    if layout.is::<Struct>() {
        let fields = layout.dtype().as_struct_fields_opt().ok_or_else(|| {
            vortex_err!("struct layout has a non-struct dtype {}", layout.dtype())
        })?;
        if layout.dtype().is_nullable()
            && let Some(validity) = layout.slot(0)?
        {
            collect_lazy_splits(&validity, root_offset, splits)?;
        }
        for idx in 0..fields.nfields() {
            let field = layout
                .slot(idx + 1)?
                .ok_or_else(|| vortex_err!("struct layout has no child for field {idx}"))?;
            collect_lazy_splits(&field, root_offset, splits)?;
        }
        return Ok(());
    }
    if layout.is::<Chunked>() {
        let chunked = layout.as_::<Chunked>();
        let mut offset = 0;
        for idx in 0..chunked.nchildren() {
            let rows = chunked.child_row_count(idx);
            if !chunked.children().child_is_indivisible(idx) {
                let child = chunked
                    .slot(idx)?
                    .ok_or_else(|| vortex_err!("chunked layout has no child {idx}"))?;
                collect_lazy_splits(&child, root_offset + offset, splits)?;
            }
            offset += rows;
            splits.push(root_offset + offset);
        }
        return Ok(());
    }
    vortex_bail!(
        "the morsel executor supports flat and chunked columns only, got {} at row offset {}",
        layout.encoding_id(),
        root_offset
    )
}

fn build_plan_inner(
    layout: &LayoutRef,
    projection: &Expression,
    filter: Option<&Expression>,
    mode: ConjunctMode,
    planned_ranges: Option<Arc<[Range<u64>]>>,
) -> VortexResult<ExecPlan> {
    let root_dtype = layout.dtype().clone();
    let root_fields = root_dtype
        .as_struct_fields_opt()
        .ok_or_else(|| vortex_err!("the morsel executor requires a struct-rooted layout"))?
        .clone();
    if root_dtype.is_nullable() {
        vortex_bail!("the morsel executor does not support a nullable root struct");
    }
    if !layout.is::<Struct>() {
        vortex_bail!(
            "the morsel executor requires a struct root layout, got {}",
            layout.encoding_id()
        );
    }

    let mut builder = Builder {
        nodes: Vec::new(),
        layout: LayoutRef::clone(layout),
        root_fields,
        splits: Vec::new(),
        planned_ranges: planned_ranges.clone(),
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
            Some(builder.push(NodeSpec::Conjunct { slots, mode }))
        }
    };

    // The projection.
    let (projection_input, projection_bound) = builder.build_scoped(projection)?;
    let output_dtype = projection_bound.dtype().clone();
    let root = builder.push(NodeSpec::Filter {
        predicate,
        projection: projection_input,
        expr: projection_bound,
        dtype: output_dtype.clone(),
    });

    let row_count = layout.row_count();
    let mut natural_splits = builder.splits;
    natural_splits.push(row_count);
    natural_splits.sort_unstable();
    natural_splits.dedup();
    natural_splits.retain(|&split| split > 0 && split <= row_count);

    Ok(ExecPlan {
        nodes: builder.nodes,
        root,
        output_dtype,
        row_count,
        natural_splits,
        planned_ranges,
    })
}

struct Builder {
    nodes: Vec<NodeSpec>,
    layout: LayoutRef,
    root_fields: StructFields,
    splits: Vec<u64>,
    planned_ranges: Option<Arc<[Range<u64>]>>,
}

impl Builder {
    fn push(&mut self, spec: NodeSpec) -> NodeId {
        self.nodes.push(spec);
        NodeId::try_from(self.nodes.len() - 1).vortex_expect("exec plan exceeds u32 nodes")
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
            let field_layout = self.field_layout(idx)?;
            children.push(self.build_layout(&field_layout, 0)?);
        }

        let node = self.push(NodeSpec::Struct {
            names: FieldNames::from(names),
            children: Arc::from(children),
            validity: None,
        });
        Ok((node, bound))
    }

    /// The struct layout's child for field `idx`, accounting for the validity slot.
    fn field_layout(&self, idx: usize) -> VortexResult<LayoutRef> {
        self.layout
            .slot(idx + 1)?
            .ok_or_else(|| vortex_err!("struct layout has no child for field {idx}"))
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

    /// Build the subtree for one column, recording its chunk boundaries as natural splits.
    fn build_layout(&mut self, layout: &LayoutRef, root_offset: u64) -> VortexResult<NodeId> {
        self.build_layout_with_lease(layout, root_offset, None)
    }

    fn build_layout_with_lease(
        &mut self,
        layout: &LayoutRef,
        root_offset: u64,
        lease_range: Option<Range<u64>>,
    ) -> VortexResult<NodeId> {
        if layout.is::<Zoned>() || layout.is::<LegacyStats>() {
            let data = layout
                .slot(0)?
                .ok_or_else(|| vortex_err!("zoned layout has no data child"))?;
            return self.build_layout_with_lease(&data, root_offset, lease_range);
        }

        if layout.is::<Flat>() {
            if lease_range.is_none() {
                self.splits.push(root_offset + layout.row_count());
            }
            let flat = layout.as_::<Flat>().clone();
            return Ok(self.push(NodeSpec::Flat {
                layout: flat,
                root_offset,
                lease_range: lease_range
                    .unwrap_or_else(|| root_offset..root_offset + layout.row_count()),
            }));
        }

        if layout.is::<Dict>() {
            let values_layout = layout
                .slot(0)?
                .ok_or_else(|| vortex_err!("dictionary layout has no values child"))?;
            let codes_layout = layout
                .slot(1)?
                .ok_or_else(|| vortex_err!("dictionary layout has no codes child"))?;
            let values_len = usize::try_from(values_layout.row_count())
                .map_err(|_| vortex_err!("dictionary values row count exceeds usize"))?;
            let logical_range = root_offset..root_offset + layout.row_count();
            let values =
                self.build_layout_with_lease(&values_layout, root_offset, Some(logical_range))?;
            let codes = self.build_layout_with_lease(&codes_layout, root_offset, lease_range)?;
            return Ok(self.push(NodeSpec::Dict {
                values,
                codes,
                values_len,
            }));
        }

        if layout.is::<Struct>() {
            let fields = layout.dtype().as_struct_fields_opt().ok_or_else(|| {
                vortex_err!("struct layout has a non-struct dtype {}", layout.dtype())
            })?;
            let names = fields.names().clone();
            let validity = if layout.dtype().is_nullable() {
                let validity_layout = layout
                    .slot(0)?
                    .ok_or_else(|| vortex_err!("nullable struct layout has no validity child"))?;
                Some(self.build_layout_with_lease(
                    &validity_layout,
                    root_offset,
                    lease_range.clone(),
                )?)
            } else {
                None
            };
            let mut children = Vec::with_capacity(fields.nfields());
            for idx in 0..fields.nfields() {
                let field = layout
                    .slot(idx + 1)?
                    .ok_or_else(|| vortex_err!("struct layout has no child for field {idx}"))?;
                children.push(self.build_layout_with_lease(
                    &field,
                    root_offset,
                    lease_range.clone(),
                )?);
            }
            return Ok(self.push(NodeSpec::Struct {
                names,
                children: Arc::from(children),
                validity,
            }));
        }

        if layout.is::<Chunked>() {
            let chunked = layout.as_::<Chunked>();
            let nchunks = chunked.nchildren();
            let mut offsets = Vec::with_capacity(nchunks + 1);
            offsets.push(0u64);
            for idx in 0..nchunks {
                offsets.push(offsets[idx] + chunked.child_row_count(idx));
            }

            let mut child_chunks = Vec::with_capacity(nchunks);
            let mut children = Vec::with_capacity(nchunks);
            for idx in 0..nchunks {
                let offset = offsets[idx];
                let child_end = offsets[idx + 1];
                let child_range = root_offset + offset..root_offset + child_end;
                if lease_range.is_none()
                    && self.planned_ranges.as_deref().is_some_and(|ranges| {
                        let candidate =
                            ranges.partition_point(|range| range.end <= child_range.start);
                        ranges
                            .get(candidate)
                            .is_none_or(|range| range.start >= child_range.end)
                    })
                {
                    continue;
                }
                let child = layout
                    .slot(idx)?
                    .ok_or_else(|| vortex_err!("chunked layout has no child {idx}"))?;
                child_chunks.push(idx);
                children.push(self.build_layout_with_lease(
                    &child,
                    root_offset + offset,
                    lease_range.clone(),
                )?);
            }
            return Ok(self.push(NodeSpec::Chunked {
                chunk_offsets: Arc::from(offsets),
                child_chunks: Arc::from(child_chunks),
                children: Arc::from(children),
                dtype: layout.dtype().clone(),
            }));
        }

        vortex_bail!(
            "the morsel executor supports flat and chunked columns only, got {} at row offset {}",
            layout.encoding_id(),
            root_offset
        )
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
