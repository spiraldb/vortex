// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Column parallelism for scans with too few row splits to occupy every worker thread.
//!
//! A wide projection over few rows, such as a small file with thousands of columns, has a single
//! row split, so one task would lower, read and decode every column. Such a scan instead
//! partitions the projection's top-level fields into contiguous column groups. Every split then
//! runs one task per group, which reads the group's columns and converts each to the scan's
//! output, and a final task zips the converted columns into one batch per split.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldName;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::StructFields;
use vortex_array::scalar_fn::fns::get_item::GetItem;
use vortex_array::scalar_fn::fns::pack::Pack as PackFn;
use vortex_array::validity::Validity;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_io::request::IoService;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;
use vortex_utils::aliases::hash_set::HashSet;

use crate::LayoutChildType;
use crate::LayoutRef;
use crate::plan::physical::Eval;
use crate::plan::physical::Pack;
use crate::plan::physical::PackPlan;
use crate::plan::physical::PlanChildren;
use crate::plan::physical::PlanRef;
use crate::scan::execution::ScanPlans;
use crate::scan::v2::split::SplitPlan;

/// A column group that reads fewer estimated bytes than this is not worth a task of its own.
const MIN_GROUP_BYTES: u64 = 8 * 1024;

/// The number of column groups that gives every worker thread a task, at most one per field.
///
/// A scan with at least one split per thread already has enough row parallelism and keeps its
/// projection whole.
pub(super) fn column_group_count(nfields: usize, splits: usize, threads: usize) -> usize {
    if splits == 0 {
        return 1;
    }
    threads.div_ceil(splits).clamp(1, nfields.max(1))
}

/// The most column groups that each read at least [`MIN_GROUP_BYTES`] of a file of `bytes`
/// serialized bytes, of which the scan reads `projected` of `nfields` root fields for
/// `selected_rows` of `row_count` rows.
pub(super) fn max_groups_for_bytes(
    bytes: u64,
    projected: usize,
    nfields: usize,
    selected_rows: u64,
    row_count: u64,
) -> usize {
    let selected = u128::from(bytes) * projected as u128 * u128::from(selected_rows)
        / (nfields.max(1) as u128 * u128::from(row_count.max(1)));
    usize::try_from(selected / u128::from(MIN_GROUP_BYTES)).unwrap_or(usize::MAX)
}

/// Partitions `nfields` fields into `groups` contiguous ranges whose sizes differ by at most one.
///
/// Neighboring columns are usually written next to each other, so a contiguous group's reads
/// can be coalesced.
pub(super) fn field_ranges(nfields: usize, groups: usize) -> Vec<Range<usize>> {
    let groups = groups.clamp(1, nfields.max(1));
    (0..groups)
        .map(|group| group * nfields / groups..(group + 1) * nfields / groups)
        .filter(|fields| !fields.is_empty())
        .collect()
}

/// A column of one split, converted to the scan's output on its group's task.
pub(super) enum Column {
    /// A Vortex array, left as read.
    Vortex(ArrayRef),
    /// An Arrow array.
    Arrow(arrow_array::ArrayRef),
}

/// Converts the column of one top-level field, by index, on its group's task.
pub(super) type ConvertColumn =
    dyn Fn(usize, ArrayRef, &mut ExecutionCtx) -> VortexResult<Column> + Send + Sync;

/// Zips a split's converted columns, in field order, into one output batch.
pub(super) type ZipColumns<A> = dyn Fn(Vec<Column>) -> VortexResult<A> + Send + Sync;

/// How a column-split scan produces its output batches from the columns of a split.
pub(super) struct ColumnOutput<A> {
    pub(super) convert: Arc<ConvertColumn>,
    pub(super) zip: Arc<ZipColumns<A>>,
}

impl<A> Clone for ColumnOutput<A> {
    fn clone(&self) -> Self {
        Self {
            convert: Arc::clone(&self.convert),
            zip: Arc::clone(&self.zip),
        }
    }
}

impl<A: 'static> ColumnOutput<A> {
    /// Leaves each column as read and maps the struct of `fields` they zip into with `map_fn`.
    pub(super) fn vortex(
        fields: StructFields,
        map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    ) -> Self {
        Self {
            convert: Arc::new(|_, column, _| Ok(Column::Vortex(column))),
            zip: Arc::new(move |columns| {
                let columns = columns
                    .into_iter()
                    .map(|column| match column {
                        Column::Vortex(column) => Ok(column),
                        Column::Arrow(_) => vortex_bail!("Expected a Vortex column"),
                    })
                    .collect::<VortexResult<Vec<_>>>()?;
                let len = columns.first().map_or(0, |column| column.len());
                let array = StructArray::try_new_with_dtype(
                    columns,
                    fields.clone(),
                    len,
                    Validity::NonNullable,
                )?;
                map_fn(array.into_array())
            }),
        }
    }
}

/// A projection split into column groups, each planned on its own.
pub(super) struct ColumnSplit {
    pub(super) groups: Vec<ColumnGroup>,
}

/// Some of a projection's top-level fields, planned as a projection of their own.
pub(super) struct ColumnGroup {
    /// The group's fields, as a range of the whole projection's fields.
    pub(super) fields: Range<usize>,
    /// Plans the projection of the group's fields.
    pub(super) plans: ScanPlans,
}

impl ColumnSplit {
    /// Splits the projection of `plans` into column groups, if it assembles a non-nullable struct
    /// from fields the root `layout` stores apart, and its `splits` row splits leave some of
    /// `threads` idle.
    ///
    /// Each group packs the plans of its own fields, which stay lazy, so a group lowers only its
    /// own fields, on its own task.
    pub(super) fn try_plan(
        plans: &ScanPlans,
        layout: &LayoutRef,
        splits: usize,
        selected_rows: u64,
        threads: usize,
    ) -> VortexResult<Option<Self>> {
        let Some(projected) = ProjectedFields::try_new(&plans.projection)? else {
            return Ok(None);
        };
        // The columns of a flat struct share their segments, so every group would read them all.
        let stored: HashSet<FieldName> = (0..layout.nslots())
            .filter_map(|slot| match layout.slot_type(slot) {
                Some(LayoutChildType::Field(field)) => Some(field),
                _ => None,
            })
            .collect();
        if !projected
            .sources
            .iter()
            .all(|source| stored.contains(source))
        {
            return Ok(None);
        }

        let fields = plans.projection.dtype().as_struct_fields();
        let bytes = plans
            .locations
            .iter()
            .map(|location| u64::from(location.length))
            .sum();
        let groups =
            column_group_count(fields.nfields(), splits, threads).min(max_groups_for_bytes(
                bytes,
                fields.nfields(),
                stored.len(),
                selected_rows,
                layout.row_count(),
            ));
        if groups < 2 {
            return Ok(None);
        }
        let row_count = plans.projection.row_count();
        let groups = field_ranges(fields.nfields(), groups)
            .into_iter()
            .map(|range| {
                let group_fields = StructFields::from_iter(range.clone().map(|field| {
                    let dtype = fields
                        .field_by_index(field)
                        .vortex_expect("grouped fields belong to the projection");
                    (fields.names()[field].clone(), dtype)
                }));
                let children = projected.children.clone();
                let indices: Arc<[usize]> = projected.indices[range.clone()].into();
                let lazy = PlanChildren::lazy(indices.len(), move |child| {
                    children
                        .get(indices[child])?
                        .ok_or_else(|| vortex_err!("Struct field {child} has no plan"))
                });
                let mut group_plans = plans.clone();
                // SAFETY: every child is the plan of a projected field, which produces the
                // field's dtype over the projection's rows, and the struct is non-nullable.
                group_plans.projection = unsafe {
                    PackPlan::from_children_unchecked(
                        group_fields,
                        Nullability::NonNullable,
                        row_count,
                        lazy,
                    )
                }
                .into_plan();
                ColumnGroup {
                    fields: range,
                    plans: group_plans,
                }
            })
            .collect();
        Ok(Some(Self { groups }))
    }
}

/// The plans of a projection's top-level fields, for a projection that assembles a non-nullable
/// struct from one plan per field.
///
/// That is a root projection, which lowers to a pack of the root's fields, or a selection of root
/// fields, which plans to a pack expression of the fields of that pack.
struct ProjectedFields {
    /// The children of the pack the fields are taken from.
    children: PlanChildren,
    /// The child of each projected field, in projection order.
    indices: Vec<usize>,
    /// The name of the root field each projected field reads, in projection order.
    sources: Vec<FieldName>,
}

impl ProjectedFields {
    fn try_new(projection: &PlanRef) -> VortexResult<Option<Self>> {
        if !matches!(
            projection.dtype(),
            DType::Struct(_, Nullability::NonNullable)
        ) {
            return Ok(None);
        }
        if let Some(pack) = projection.as_opt::<Pack>() {
            return Ok(Some(Self {
                children: pack.children().clone(),
                indices: (0..pack.nfields()).collect(),
                sources: pack.fields().names().iter().cloned().collect(),
            }));
        }
        let Some(eval) = projection.as_opt::<Eval>() else {
            return Ok(None);
        };
        let expression = eval.expression();
        let Some(child) = eval.child(0)? else {
            return Ok(None);
        };
        let Some(pack) = child.as_opt::<Pack>() else {
            return Ok(None);
        };
        if !expression.is::<PackFn>() || pack.dtype().is_nullable() {
            return Ok(None);
        }
        let positions: HashMap<&FieldName, usize> = pack
            .fields()
            .names()
            .iter()
            .enumerate()
            .map(|(field, name)| (name, field))
            .collect();
        let mut indices = Vec::with_capacity(expression.children().len());
        let mut sources = Vec::with_capacity(expression.children().len());
        for field in expression.children() {
            let source = match field.as_opt::<GetItem>() {
                Some(source) if field.child(0).is_root() => source,
                _ => return Ok(None),
            };
            let Some(&index) = positions.get(source) else {
                return Ok(None);
            };
            indices.push(index);
            sources.push(source.clone());
        }
        Ok(Some(Self {
            children: pack.children().clone(),
            indices,
            sources,
        }))
    }
}

/// Runs a group's split and converts each of its columns, the projection's `fields`, whose
/// dtypes are `struct_fields`.
pub(super) async fn run_group(
    session: VortexSession,
    split: SplitPlan,
    io: Arc<dyn IoService>,
    fields: Range<usize>,
    struct_fields: StructFields,
    convert: Arc<ConvertColumn>,
) -> VortexResult<Vec<Column>> {
    let batches = split.run(io).await?;
    let mut ctx = session.create_execution_ctx();
    let structs = batches
        .into_iter()
        .map(|batch| batch.execute::<StructArray>(&mut ctx))
        .collect::<VortexResult<Vec<_>>>()?;
    fields
        .zip(struct_fields.fields())
        .enumerate()
        .map(|(column, (field, dtype))| {
            let array = match structs.as_slice() {
                [batch] => batch.unmasked_field(column).clone(),
                batches => ChunkedArray::try_new(
                    batches
                        .iter()
                        .map(|batch| batch.unmasked_field(column).clone()),
                    dtype,
                )?
                .into_array(),
            };
            convert(field, array, &mut ctx)
        })
        .collect()
}
