// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Exact row selection for CUDA scans whose filter otherwise only prunes zones.
//!
//! [`scan_cuda_pruned`](crate::scan_cuda_pruned) skips zones a filter rules out, but returns kept
//! zones whole, so a consumer that filters afterwards copies and evaluates every row of a kept
//! zone. [`select_rows`] additionally evaluates the filter's comparisons of a field with a
//! literal on the GPU, and drops the failing rows from each decoded batch before export.
//!
//! Compatible single-field conjunctions use a typed mask by default, followed by the
//! existing count and per-column CUB selection. Other conjunctions use per-comparison masks.
//! `VORTEX_CUDA_TYPED_FILTER=0` disables typed masks; `VORTEX_CUDA_EXACT_FILTER_IMPL=cub`
//! takes precedence and also forces per-comparison masks. The consumer still filters finally.

use std::env;
use std::sync::Arc;

use cudarc::driver::CudaSlice;
use cudarc::driver::CudaView;
use cudarc::driver::DeviceRepr;
use cudarc::driver::LaunchConfig;
use cudarc::driver::PushKernelArg;
use futures::StreamExt;
use vortex::array::ArrayRef;
use vortex::array::ArrayView;
use vortex::array::Canonical;
use vortex::array::IntoArray;
use vortex::array::arrays::ExtensionArray;
use vortex::array::arrays::Primitive;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::Struct;
use vortex::array::arrays::StructArray;
use vortex::array::arrays::extension::ExtensionArrayExt;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::array::buffer::BufferHandle;
use vortex::array::buffer::DeviceBuffer;
use vortex::array::expr::Expression;
use vortex::array::expr::forms::conjuncts;
use vortex::array::match_each_native_simd_ptype;
use vortex::array::scalar_fn::fns::binary::Binary;
use vortex::array::scalar_fn::fns::get_item::GetItem;
use vortex::array::scalar_fn::fns::literal::Literal;
use vortex::array::scalar_fn::fns::operators::CompareOperator;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::array::stream::SendableArrayStream;
use vortex::array::validity::Validity;
use vortex::buffer::Alignment;
use vortex::dtype::DType;
use vortex::dtype::FieldName;
use vortex::dtype::PType;
use vortex::dtype::StructFields;
use vortex::error::VortexResult;
use vortex::error::vortex_err;
use vortex::scalar::PValue;
use vortex::session::VortexSession;

use crate::CudaBufferExt;
use crate::CudaDeviceBuffer;
use crate::CudaSession;
use crate::cub::select_bitmask;
use crate::executor::CudaArrayExt;
use crate::executor::CudaExecutionCtx;
use crate::file::coerce_comparison_literals;

// Keep the launch dimensions and descriptor layouts in sync with exact_filter.cu.
const FILTER_THREADS: usize = 256;
const FILTER_TILE_ROWS: usize = 2048;
const FILTER_MAX_PREDICATES: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SelectionImpl {
    Generic,
    Typed,
}

impl SelectionImpl {
    fn from_env() -> Self {
        Self::from_settings(
            env::var("VORTEX_CUDA_EXACT_FILTER_IMPL").ok().as_deref(),
            env::var("VORTEX_CUDA_TYPED_FILTER").ok().as_deref(),
        )
    }

    fn from_settings(implementation: Option<&str>, typed_filter: Option<&str>) -> Self {
        match (implementation, typed_filter) {
            (Some("cub"), _) | (_, Some("0")) => Self::Generic,
            _ => Self::Typed,
        }
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct FilterComparison {
    literal: u64,
    operator: u32,
    reserved: u32,
}

#[repr(C)]
struct FilterPredicates {
    comparisons: [FilterComparison; FILTER_MAX_PREDICATES],
}

// SAFETY: Matches the integer-only C layout in exact_filter.cu, with explicit padding.
unsafe impl DeviceRepr for FilterPredicates {}

/// One conjunct of a scan filter: a projected field compared with a non-null literal.
#[derive(Clone, Debug)]
struct FieldComparison {
    /// Position of the compared field in each batch.
    index: usize,
    /// Comparison of the field with the literal, with the field on the left.
    operator: CompareOperator,
    /// Non-null literal in the field's primitive storage type.
    literal: PValue,
}

#[allow(non_upper_case_globals, non_camel_case_types, clippy::absolute_paths)]
mod compare_abi {
    include!(concat!(env!("OUT_DIR"), "/compare.rs"));
}

/// Convert a logical comparison to the opcode defined in the shared CUDA header.
fn compare_opcode(operator: CompareOperator) -> compare_abi::CompareOp {
    match operator {
        CompareOperator::Eq => compare_abi::CompareOp_COMPARE_EQ,
        CompareOperator::NotEq => compare_abi::CompareOp_COMPARE_NOT_EQ,
        CompareOperator::Gt => compare_abi::CompareOp_COMPARE_GT,
        CompareOperator::Gte => compare_abi::CompareOp_COMPARE_GTE,
        CompareOperator::Lt => compare_abi::CompareOp_COMPARE_LT,
        CompareOperator::Lte => compare_abi::CompareOp_COMPARE_LTE,
    }
}

/// Drop the rows of each batch of `stream` that fail a conjunct of `filter` comparing a projected
/// primitive or temporal field with a literal, evaluated on the GPU. Other conjuncts are left to
/// the consumer, so the result is a superset of the rows satisfying `filter`, as the CUDA scan's
/// filter contract allows. Batches that would keep most rows, or hold columns other than non-null
/// primitives and temporals, keep all their rows.
///
/// Selection decodes the columns of eligible batches, so export with
/// [`DictionaryExport::Decode`](crate::DictionaryExport::Decode) for a schema that is stable
/// across batches.
///
/// `filter` is over `scope`; its comparison literals are cast to the compared column's type.
/// Up to `concurrency` batches are selected at once, matching the scan's split concurrency, so
/// selecting one batch does not hold back decoding the next.
pub fn select_rows(
    stream: SendableArrayStream,
    filter: &Expression,
    scope: &DType,
    session: VortexSession,
    concurrency: usize,
) -> VortexResult<SendableArrayStream> {
    let dtype = stream.dtype().clone();
    let comparisons: Arc<[FieldComparison]> = field_comparisons(filter, scope, &dtype)?.into();
    if comparisons.is_empty() {
        return Ok(stream);
    }
    let implementation = SelectionImpl::from_env();
    let batches = stream
        .map(move |batch| {
            let comparisons = Arc::clone(&comparisons);
            let session = session.clone();
            async move {
                let mut ctx = CudaSession::create_execution_ctx(&session)?;
                select_batch(batch?, &comparisons, implementation, &mut ctx).await
            }
        })
        .buffered(concurrency.max(1));
    Ok(Box::pin(ArrayStreamAdapter::new(dtype, batches)))
}

/// The conjuncts of `filter`, over `scope`, that compare a primitive or temporal field of
/// `projection` with a literal.
fn field_comparisons(
    filter: &Expression,
    scope: &DType,
    projection: &DType,
) -> VortexResult<Vec<FieldComparison>> {
    let filter = coerce_comparison_literals(filter.clone(), scope)?;
    let Some(fields) = projection.as_struct_fields_opt() else {
        return Ok(Vec::new());
    };
    Ok(conjuncts(&filter)
        .iter()
        .filter_map(|expr| field_comparison(expr, fields))
        .collect())
}

/// `expr` as a comparison of a primitive or temporal field in `fields` with a non-null literal
/// of the field's storage type, with the field on the left.
fn field_comparison(expr: &Expression, fields: &StructFields) -> Option<FieldComparison> {
    let operator = expr.as_opt::<Binary>()?.to_compare_operator()?;
    let (lhs, rhs) = (expr.child(0), expr.child(1));
    let (field, literal, operator) = match (field_of(lhs), rhs.as_opt::<Literal>()) {
        (Some(field), Some(literal)) => (field, literal, operator),
        _ => match (field_of(rhs), lhs.as_opt::<Literal>()) {
            (Some(field), Some(literal)) => (field, literal, operator.swap()),
            _ => return None,
        },
    };
    let index = fields.find(field)?;
    let ptype = primitive_storage(&fields.field_by_index(index)?)?;
    let literal = match literal.dtype() {
        DType::Extension(_) => literal.as_extension().to_storage_scalar(),
        _ => literal.clone(),
    };
    let literal = literal.as_primitive_opt()?.pvalue()?;
    (literal.ptype() == ptype).then_some(FieldComparison {
        index,
        operator,
        literal,
    })
}

/// The field `expr` reads from the scope root, if it is a plain field reference.
fn field_of(expr: &Expression) -> Option<&FieldName> {
    let field = expr.as_opt::<GetItem>()?;
    expr.child(0).is_root().then_some(field)
}

/// If `batch` is a struct of primitive and temporal columns, decode it, and if its columns hold
/// no nulls and at most half its rows satisfy all `comparisons`, keep only those rows.
async fn select_batch(
    batch: ArrayRef,
    comparisons: &[FieldComparison],
    implementation: SelectionImpl,
    ctx: &mut CudaExecutionCtx,
) -> VortexResult<ArrayRef> {
    let len = batch.len();
    let Some(batch_struct) = batch.as_opt::<Struct>() else {
        return Ok(batch);
    };
    let validity = batch_struct.validity()?;
    // Only primitive and temporal columns are compacted. Others, like strings, keep their
    // batch whole before any work, so they also keep their direct export paths.
    let compactable = batch_struct
        .struct_fields()
        .fields()
        .all(|dtype| primitive_storage(&dtype).is_some());
    if len == 0 || !compactable || !matches!(validity, Validity::NonNullable | Validity::AllValid) {
        return Ok(batch);
    }
    let names = batch_struct.struct_fields().names().clone();
    // Export decodes every column anyway, so decoding them first adds no work, and whole and
    // selected batches share the decoded columns.
    let mut fields = Vec::with_capacity(names.len());
    for field in batch_struct.iter_unmasked_fields() {
        fields.push(field.clone().execute_cuda(ctx).await?);
    }
    let mut rows = len;
    // Columns holding nulls would need their validity selected too.
    if fields
        .iter()
        .all(|field| selectable_values(field).is_some())
    {
        let mask = selection_mask(&fields, comparisons, len, implementation, ctx).await?;
        let selected = count_selected(&mask, len, ctx).await?;
        // Compacting more than half the rows costs a pass over every column for little saving.
        if selected <= len / 2 {
            let mask = mask.cuda_view::<u8>()?;
            for field in &mut fields {
                *field = select_field(field, &mask, selected, ctx).await?;
            }
            rows = selected;
        }
    }
    Ok(StructArray::try_new(
        names,
        fields.into_iter().map(Canonical::into_array),
        rows,
        validity,
    )?
    .into_array())
}

/// The primitive type of a primitive column, or of a temporal column's storage.
fn primitive_storage(dtype: &DType) -> Option<PType> {
    let storage = match dtype {
        DType::Extension(ext) => ext.storage_dtype(),
        dtype => dtype,
    };
    match storage {
        DType::Primitive(ptype, _) if *ptype != PType::F16 => Some(*ptype),
        _ => None,
    }
}

/// The number of bits set among the first `len` of `mask`, read back without blocking the
/// calling thread.
async fn count_selected(
    mask: &BufferHandle,
    len: usize,
    ctx: &mut CudaExecutionCtx,
) -> VortexResult<usize> {
    let mut count = ctx
        .stream()
        .alloc_zeros::<u64>(1)
        .map_err(|error| vortex_err!("Failed to allocate selection count: {error}"))?;
    let mask = mask.cuda_view::<u8>()?;
    let len = u64::try_from(len)?;
    let bit_offset = 0u64;
    let function = ctx.load_function_with_suffixes("arrow_validity", &["count_valid"])?;
    ctx.launch_kernel(&function, mask.len(), |args| {
        args.arg(&mask).arg(&mut count).arg(&len).arg(&bit_offset);
    })?;
    let count = CudaDeviceBuffer::new(count)
        .copy_to_host(Alignment::of::<u64>())?
        .await?;
    let count = u64::from_le_bytes(
        count
            .as_slice()
            .try_into()
            .map_err(|_| vortex_err!("selection count has {} bytes", count.len()))?,
    );
    Ok(usize::try_from(count)?)
}

fn typed_filter_type(comparisons: &[FieldComparison]) -> Option<PType> {
    let first = comparisons.first()?;
    let ptype = first.literal.ptype();
    (comparisons.len() <= FILTER_MAX_PREDICATES
        && ptype != PType::F16
        && comparisons.iter().all(|comparison| {
            comparison.index == first.index && comparison.literal.ptype() == ptype
        }))
    .then_some(ptype)
}

fn comparison_descriptor(comparison: &FieldComparison) -> VortexResult<FilterComparison> {
    let literal = match_each_native_simd_ptype!(comparison.literal.ptype(), |T| {
        let value = comparison.literal.cast::<T>()?;
        let bytes = value.to_le_bytes();
        let mut padded = [0u8; 8];
        padded[..bytes.len()].copy_from_slice(&bytes);
        u64::from_le_bytes(padded)
    });
    Ok(FilterComparison {
        literal,
        operator: compare_opcode(comparison.operator),
        reserved: 0,
    })
}

async fn selection_mask(
    fields: &[Canonical],
    comparisons: &[FieldComparison],
    len: usize,
    implementation: SelectionImpl,
    ctx: &mut CudaExecutionCtx,
) -> VortexResult<BufferHandle> {
    let mut words = ctx.device_alloc::<u32>(len.div_ceil(32))?;
    let typed = (implementation == SelectionImpl::Typed)
        .then(|| typed_filter_type(comparisons))
        .flatten();
    if let Some(ptype) = typed {
        let values = selectable_values(&fields[comparisons[0].index])
            .ok_or_else(|| vortex_err!("compared column is not a non-null primitive"))?;
        let buffer = ctx.ensure_on_device(values.buffer_handle().clone()).await?;
        let input = buffer.cuda_view::<u8>()?;
        let mut predicates = FilterPredicates {
            comparisons: [FilterComparison::default(); FILTER_MAX_PREDICATES],
        };
        for (descriptor, comparison) in predicates.comparisons.iter_mut().zip(comparisons) {
            *descriptor = comparison_descriptor(comparison)?;
        }
        let num_predicates = u32::try_from(comparisons.len())?;
        let len_u64 = u64::try_from(len)?;
        let function =
            ctx.load_function_with_suffixes("exact_filter", &["mask", &ptype.to_string()])?;
        let config = LaunchConfig {
            grid_dim: (u32::try_from(len.div_ceil(FILTER_TILE_ROWS))?, 1, 1),
            block_dim: (u32::try_from(FILTER_THREADS)?, 1, 1),
            shared_mem_bytes: 0,
        };
        ctx.launch_kernel_config(&function, config, len, |args| {
            args.arg(&input)
                .arg(&predicates)
                .arg(&num_predicates)
                .arg(&len_u64)
                .arg(&mut words);
        })?;
    } else {
        for (position, comparison) in comparisons.iter().enumerate() {
            launch_compare(
                &fields[comparison.index],
                comparison,
                &mut words,
                position > 0,
                ctx,
            )
            .await?;
        }
    }
    Ok(BufferHandle::new_device(Arc::new(CudaDeviceBuffer::new(
        words,
    ))))
}

/// The non-null primitive values of a decoded primitive or temporal column.
fn selectable_values(canonical: &Canonical) -> Option<ArrayView<'_, Primitive>> {
    let values = match canonical {
        Canonical::Primitive(values) => values.as_view(),
        Canonical::Extension(ext) => ext.storage_array().as_opt::<Primitive>()?,
        _ => return None,
    };
    matches!(
        values.validity(),
        Ok(Validity::NonNullable | Validity::AllValid)
    )
    .then_some(values)
}

/// Keep the rows of a decoded column whose bit is set in `mask`.
async fn select_field(
    field: &Canonical,
    mask: &CudaView<'_, u8>,
    selected: usize,
    ctx: &mut CudaExecutionCtx,
) -> VortexResult<Canonical> {
    let values = selectable_values(field)
        .ok_or_else(|| vortex_err!("selected column is not a non-null primitive"))?;
    let ptype = values.ptype();
    let validity = values.validity()?;
    let input = ctx.ensure_on_device(values.buffer_handle().clone()).await?;
    let output = match_each_native_simd_ptype!(ptype, |T| {
        // SAFETY: Comparison kernels initialized the mask and cleared its tail bits. The count
        // was read from that same mask, and the input values are decoded on this stream.
        unsafe { select_bitmask(input.cuda_view::<T>()?, mask, 0, selected, ctx) }?
    });
    let values = PrimitiveArray::from_buffer_handle(output, ptype, validity);
    Ok(match field {
        Canonical::Extension(ext) => Canonical::Extension(ExtensionArray::new(
            ext.ext_dtype().clone(),
            values.into_array(),
        )),
        _ => Canonical::Primitive(values),
    })
}

/// Write `field <op> literal` into `words`, or AND it into them if `combine` is set.
async fn launch_compare(
    field: &Canonical,
    comparison: &FieldComparison,
    words: &mut CudaSlice<u32>,
    combine: bool,
    ctx: &mut CudaExecutionCtx,
) -> VortexResult<()> {
    let values = selectable_values(field)
        .ok_or_else(|| vortex_err!("compared column is not a non-null primitive"))?;
    let ptype = values.ptype();
    let len = values.len();
    let buffer = ctx.ensure_on_device(values.buffer_handle().clone()).await?;
    let len_u64 = len as u64;
    let operator = compare_opcode(comparison.operator);
    let combine = u32::from(combine);
    let function = ctx.load_function("compare", &[ptype])?;
    match_each_native_simd_ptype!(ptype, |T| {
        let literal = comparison.literal.cast::<T>()?;
        let values = buffer.cuda_view::<T>()?;
        ctx.launch_kernel(&function, len, |args| {
            args.arg(&values)
                .arg(&len_u64)
                .arg(&operator)
                .arg(&literal)
                .arg(words)
                .arg(&combine);
        })
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use vortex::array::VortexSessionExecute;
    use vortex::array::array_session;
    use vortex::array::arrays::TemporalArray;
    use vortex::array::arrays::VarBinViewArray;
    use vortex::array::assert_arrays_eq;
    use vortex::array::expr::and;
    use vortex::array::expr::col;
    use vortex::array::expr::eq;
    use vortex::array::expr::gt;
    use vortex::array::expr::gt_eq;
    use vortex::array::expr::lit;
    use vortex::array::expr::lt;
    use vortex::array::expr::or;
    use vortex::array::extension::datetime::TimeUnit;
    use vortex::dtype::Nullability;
    use vortex::error::VortexExpect;
    use vortex::mask::Mask;

    use super::*;
    use crate::CanonicalCudaExt;
    use crate::CudaSession;
    use crate::cuda_session;

    /// Columns `x = 0..5000` and `y = x / 2` as f64.
    fn batch() -> VortexResult<ArrayRef> {
        let x = PrimitiveArray::from_iter(0i32..5000).into_array();
        let y = PrimitiveArray::from_iter((0..5000).map(|x| f64::from(x) / 2.0)).into_array();
        Ok(StructArray::from_fields(&[("x", x), ("y", y)])?.into_array())
    }

    async fn select(batch: ArrayRef, filter: Expression) -> VortexResult<ArrayRef> {
        select_with_impl(batch, filter, SelectionImpl::Typed).await
    }

    async fn select_with_impl(
        batch: ArrayRef,
        filter: Expression,
        implementation: SelectionImpl,
    ) -> VortexResult<ArrayRef> {
        let comparisons = field_comparisons(&filter, batch.dtype(), batch.dtype())?;
        let mut ctx = CudaSession::create_execution_ctx(&cuda_session())?;
        let selected = select_batch(batch, &comparisons, implementation, &mut ctx).await?;
        Ok(selected
            .execute_cuda(&mut ctx)
            .await?
            .into_host(ctx.execution_ctx())
            .await?
            .into_array())
    }

    /// A selective conjunction keeps exactly the matching rows of every column.
    #[rstest]
    #[case::empty(1000, 1000)]
    #[case::partial_word(1000, 1033)]
    #[case::multiple_words(1000, 1500)]
    #[case::half(0, 2500)]
    #[crate::test]
    async fn selective_comparisons_drop_failing_rows(
        #[case] start: i32,
        #[case] end: i32,
    ) -> VortexResult<()> {
        let filter = and(
            gt_eq(col("x"), lit(i64::from(start))),
            lt(col("x"), lit(end)),
        );
        let selected = select(batch()?, filter).await?;
        let x = PrimitiveArray::from_iter(start..end).into_array();
        let y = PrimitiveArray::from_iter((start..end).map(|x| f64::from(x) / 2.0)).into_array();
        let expected = StructArray::from_fields(&[("x", x), ("y", y)])?.into_array();
        let mut ctx = array_session().create_execution_ctx();
        assert_arrays_eq!(selected, expected, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[case::u8(PType::U8)]
    #[case::u16(PType::U16)]
    #[case::u32(PType::U32)]
    #[case::u64(PType::U64)]
    #[case::i8(PType::I8)]
    #[case::i16(PType::I16)]
    #[case::i32(PType::I32)]
    #[case::i64(PType::I64)]
    #[case::f32(PType::F32)]
    #[case::f64(PType::F64)]
    #[crate::test]
    async fn test_numeric_dispatch(#[case] ptype: PType) -> VortexResult<()> {
        let values = match_each_native_simd_ptype!(ptype, |T| {
            PrimitiveArray::from_iter((0u8..33).map(|i| T::try_from(i).unwrap())).into_array()
        });
        let expected = StructArray::from_fields(&[("x", values.slice(0..10)?)])?.into_array();
        let batch = StructArray::from_fields(&[("x", values)])?.into_array();
        let selected = select(batch, lt(col("x"), lit(10u8))).await?;
        let mut ctx = array_session().create_execution_ctx();
        assert_arrays_eq!(selected, expected, &mut ctx);
        Ok(())
    }

    /// A literal on the left swaps the operator; a float column compares as floats.
    #[crate::test]
    async fn literal_on_the_left_swaps_the_operator() -> VortexResult<()> {
        let selected = select(batch()?, gt(lit(100.0f64), col("y"))).await?;
        assert_eq!(selected.len(), 200);
        Ok(())
    }

    /// Batches that would keep most rows are returned whole.
    #[crate::test]
    async fn unselective_comparisons_keep_the_batch() -> VortexResult<()> {
        let selected = select(batch()?, lt(col("x"), lit(4000i32))).await?;
        assert_eq!(selected.len(), 5000);
        Ok(())
    }

    /// Dates compare on their day-count storage.
    #[rstest]
    #[crate::test]
    async fn dates_compare_on_storage(
        #[values(SelectionImpl::Generic, SelectionImpl::Typed)] implementation: SelectionImpl,
    ) -> VortexResult<()> {
        let days = PrimitiveArray::from_iter(0i32..5000).into_array();
        let date = TemporalArray::new_date(days, TimeUnit::Days).into_array();
        let batch = StructArray::from_fields(&[("d", date)])?.into_array();
        let selected =
            select_with_impl(batch, gt_eq(col("d"), lit(4500i32)), implementation).await?;
        assert_eq!(selected.len(), 500);
        Ok(())
    }

    /// Columns other than non-null primitives are not compacted, so their batches stay whole.
    #[crate::test]
    async fn batches_with_strings_stay_whole() -> VortexResult<()> {
        let x = PrimitiveArray::from_iter(0i32..5000).into_array();
        let s = VarBinViewArray::from_iter_str((0..5000).map(|i| format!("s{i}"))).into_array();
        let batch = StructArray::from_fields(&[("x", x), ("s", s)])?.into_array();
        let selected = select(batch, lt(col("x"), lit(100i32))).await?;
        assert_eq!(selected.len(), 5000);
        Ok(())
    }

    /// Disjunctions, computed operands and unprojected fields are not comparisons of a batch's
    /// field with a literal.
    #[test]
    fn other_predicates_select_nothing() -> VortexResult<()> {
        let dtype = batch()?.dtype().clone();
        let disjunction = or(lt(col("x"), lit(1i32)), gt(col("x"), lit(4998i32)));
        assert!(field_comparisons(&disjunction, &dtype, &dtype)?.is_empty());
        let columns = lt(col("x"), col("y"));
        assert!(field_comparisons(&columns, &dtype, &dtype)?.is_empty());
        let y = dtype.as_struct_fields().project(&["y".into()])?;
        let projection = DType::Struct(y, Nullability::NonNullable);
        let unprojected = lt(col("x"), lit(1i32));
        assert!(field_comparisons(&unprojected, &dtype, &projection)?.is_empty());
        Ok(())
    }
    struct TestMask {
        words: BufferHandle,
        selected: usize,
    }

    async fn test_selection_mask(
        inputs: Vec<(ArrayView<'_, Primitive>, &FieldComparison)>,
        len: usize,
        typed: bool,
        ctx: &mut CudaExecutionCtx,
    ) -> VortexResult<TestMask> {
        let fields = vec![Canonical::Primitive(PrimitiveArray::from_buffer_handle(
            inputs[0].0.buffer_handle().clone(),
            inputs[0].0.ptype(),
            inputs[0].0.validity()?,
        ))];
        let comparisons: Vec<_> = inputs
            .iter()
            .map(|(_, comparison)| (*comparison).clone())
            .collect();
        let implementation = if typed {
            SelectionImpl::Typed
        } else {
            SelectionImpl::Generic
        };
        let words = selection_mask(&fields, &comparisons, len, implementation, ctx).await?;
        let selected = count_selected(&words, len, ctx).await?;
        Ok(TestMask { words, selected })
    }

    #[test]
    fn test_typed_filter_eligibility() {
        let comparison = FieldComparison {
            index: 0,
            operator: CompareOperator::Eq,
            literal: PValue::I32(1),
        };
        assert_eq!(typed_filter_type(&[]), None);
        assert_eq!(typed_filter_type(&[comparison.clone()]), Some(PType::I32));
        assert_eq!(
            typed_filter_type(&vec![comparison.clone(); FILTER_MAX_PREDICATES]),
            Some(PType::I32)
        );
        assert_eq!(
            typed_filter_type(&vec![comparison.clone(); FILTER_MAX_PREDICATES + 1]),
            None
        );
        let mut other = comparison.clone();
        other.index = 1;
        assert_eq!(typed_filter_type(&[comparison.clone(), other]), None);
        let mut other = comparison.clone();
        other.literal = PValue::I64(1);
        assert_eq!(typed_filter_type(&[comparison, other]), None);
    }

    #[rstest]
    #[case(None, None, SelectionImpl::Typed)]
    #[case(None, Some("0"), SelectionImpl::Generic)]
    #[case(None, Some("true"), SelectionImpl::Typed)]
    #[case(None, Some(""), SelectionImpl::Typed)]
    #[case(None, Some("1"), SelectionImpl::Typed)]
    #[case(Some("shared"), None, SelectionImpl::Typed)]
    #[case(Some("shared"), Some("0"), SelectionImpl::Generic)]
    #[case(Some("shared"), Some("1"), SelectionImpl::Typed)]
    #[case(Some("cub"), None, SelectionImpl::Generic)]
    #[case(Some("cub"), Some("0"), SelectionImpl::Generic)]
    #[case(Some("cub"), Some("1"), SelectionImpl::Generic)]
    fn test_selection_settings(
        #[case] implementation: Option<&str>,
        #[case] typed_filter: Option<&str>,
        #[case] expected: SelectionImpl,
    ) {
        assert_eq!(
            SelectionImpl::from_settings(implementation, typed_filter),
            expected
        );
    }

    #[test]
    fn test_inline_descriptor_layouts() {
        assert_eq!(size_of::<FilterComparison>(), 16);
        assert_eq!(size_of::<FilterPredicates>(), 256);
    }

    #[rstest]
    #[case(PValue::I8(-2), 0xfe)]
    #[case(PValue::I16(-2), 0xfffe)]
    #[case(PValue::I32(-2), 0xffff_fffe)]
    #[case(PValue::I64(-2), 0xffff_ffff_ffff_fffe)]
    #[case(PValue::U64(u64::MAX), u64::MAX)]
    #[case(PValue::F32(-0.0), 0x8000_0000)]
    #[case(PValue::F64(-0.0), 0x8000_0000_0000_0000)]
    fn test_literal_descriptor_bits(
        #[case] literal: PValue,
        #[case] expected: u64,
    ) -> VortexResult<()> {
        let descriptor = comparison_descriptor(&FieldComparison {
            index: 0,
            operator: CompareOperator::Gte,
            literal,
        })?;
        assert_eq!(descriptor.literal, expected);
        assert_eq!(descriptor.operator, compare_abi::CompareOp_COMPARE_GTE);
        Ok(())
    }

    /// Every primitive width, signed/unsigned/float bit patterns, nullable-but-all-valid,
    /// and temporal storage are copied together using the same mask with per-column CUB selection.
    fn mixed_batch(len: i32) -> VortexResult<ArrayRef> {
        let rows = 0..len;
        let key = PrimitiveArray::from_iter(rows.clone().map(|row| row % 7)).into_array();
        let u8s = PrimitiveArray::from_option_iter(
            rows.clone()
                .map(|row| Some(u8::try_from(row % 251).vortex_expect("bounded remainder"))),
        )
        .into_array();
        let i8s = PrimitiveArray::from_iter(
            rows.clone()
                .map(|row| i8::try_from(row % 127 - 63).vortex_expect("bounded remainder")),
        )
        .into_array();
        let u16s = PrimitiveArray::from_iter(
            rows.clone()
                .map(|row| u16::try_from(row % 65536).vortex_expect("bounded remainder")),
        )
        .into_array();
        let i16s = PrimitiveArray::from_iter(
            rows.clone()
                .map(|row| i16::try_from(row % 1000 - 500).vortex_expect("bounded remainder")),
        )
        .into_array();
        let u32s = PrimitiveArray::from_iter(rows.clone().map(|row| 4_000_000_000u32 + row as u32))
            .into_array();
        let i64s = PrimitiveArray::from_iter(rows.clone().map(|row| i64::MIN + i64::from(row)))
            .into_array();
        let u64s =
            PrimitiveArray::from_iter(rows.clone().map(|row| u64::MAX - row as u64)).into_array();
        let f32s =
            PrimitiveArray::from_iter(rows.clone().map(|row| -(row as f32) / 10.0)).into_array();
        let f64s =
            PrimitiveArray::from_iter(rows.clone().map(|row| f64::from(row) / 10.0)).into_array();
        let dates =
            TemporalArray::new_date(PrimitiveArray::from_iter(rows).into_array(), TimeUnit::Days)
                .into_array();
        Ok(StructArray::from_fields(&[
            ("key", key),
            ("u8", u8s),
            ("i8", i8s),
            ("u16", u16s),
            ("i16", i16s),
            ("u32", u32s),
            ("i64", i64s),
            ("u64", u64s),
            ("f32", f32s),
            ("f64", f64s),
            ("d", dates),
        ])?
        .into_array())
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(31)]
    #[case(32)]
    #[case(33)]
    #[case(2047)]
    #[case(2048)]
    #[case(2049)]
    #[case(524289)]
    #[crate::test]
    async fn test_multi_column_widths_and_mask_boundaries(
        #[case] len: i32,
        #[values(SelectionImpl::Generic, SelectionImpl::Typed)] implementation: SelectionImpl,
    ) -> VortexResult<()> {
        let batch = mixed_batch(len)?;
        let expected = batch.filter(Mask::from_iter((0..len).map(|row| row % 7 == 6)))?;
        let filter = and(
            eq(col("key"), lit(6i32)),
            and(
                gt_eq(col("u32"), lit(4_000_000_000u32)),
                lt(col("f64"), lit(1.0e12f64)),
            ),
        );
        let selected = select_with_impl(batch, filter, implementation).await?;
        let mut ctx = array_session().create_execution_ctx();
        assert_arrays_eq!(selected, expected, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[case(0, 0)]
    #[case(2500, 2500)]
    #[case(2501, 5000)]
    #[case(5000, 5000)]
    #[crate::test]
    async fn test_empty_half_and_dense_masks(
        #[case] cutoff: i32,
        #[case] expected_len: usize,
        #[values(SelectionImpl::Generic, SelectionImpl::Typed)] implementation: SelectionImpl,
    ) -> VortexResult<()> {
        let batch = batch()?;
        let expected = batch.slice(0..expected_len)?;
        let selected = select_with_impl(batch, lt(col("x"), lit(cutoff)), implementation).await?;
        let mut ctx = array_session().create_execution_ctx();
        assert_arrays_eq!(selected, expected, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[case(PType::U8)]
    #[case(PType::U16)]
    #[case(PType::U32)]
    #[case(PType::U64)]
    #[case(PType::I8)]
    #[case(PType::I16)]
    #[case(PType::I32)]
    #[case(PType::I64)]
    #[case(PType::F32)]
    #[case(PType::F64)]
    #[crate::test]
    async fn test_comparison_types_and_operators(
        #[case] ptype: PType,
        #[values(false, true)] typed_mask: bool,
    ) -> VortexResult<()> {
        let mut ctx = CudaSession::create_execution_ctx(&cuda_session())?;
        for operator in [
            CompareOperator::Eq,
            CompareOperator::NotEq,
            CompareOperator::Gt,
            CompareOperator::Gte,
            CompareOperator::Lt,
            CompareOperator::Lte,
        ] {
            let (values, literal, expected) = match_each_native_simd_ptype!(ptype, |T| {
                let values = [T::MIN, 0 as T, 1 as T, T::MAX];
                let literal = 1 as T;
                let expected = values
                    .iter()
                    .enumerate()
                    .fold(0u32, |mask, (index, value)| {
                        let keep = match operator {
                            CompareOperator::Eq => *value == literal,
                            CompareOperator::NotEq => *value != literal,
                            CompareOperator::Gt => *value > literal,
                            CompareOperator::Gte => *value >= literal,
                            CompareOperator::Lt => *value < literal,
                            CompareOperator::Lte => *value <= literal,
                        };
                        mask | (u32::from(keep) << index)
                    });
                (
                    PrimitiveArray::from_iter(values),
                    PValue::from(literal),
                    expected,
                )
            });
            let comparison = FieldComparison {
                index: 0,
                operator,
                literal,
            };
            let mask = test_selection_mask(
                vec![(values.as_view(), &comparison)],
                4,
                typed_mask,
                &mut ctx,
            )
            .await?;
            let words = ctx
                .stream()
                .clone_dtoh(&mask.words.cuda_view::<u32>()?)
                .map_err(|error| vortex_err!("Failed to read selection mask: {error}"))?;
            assert_eq!(words, vec![expected]);
            assert_eq!(mask.selected, expected.count_ones() as usize);
        }
        Ok(())
    }

    #[rstest]
    #[case(PType::I8, i64::from(i8::MIN), i64::from(i8::MAX))]
    #[case(PType::I16, i64::from(i16::MIN), i64::from(i16::MAX))]
    #[case(PType::I32, i64::from(i32::MIN), i64::from(i32::MAX))]
    #[case(PType::I64, i64::MIN, i64::MAX)]
    #[crate::test]
    async fn test_signed_literal_reconstruction_all_operators(
        #[case] ptype: PType,
        #[case] min: i64,
        #[case] max: i64,
        #[values(false, true)] typed_mask: bool,
    ) -> VortexResult<()> {
        // The oracle compares original signed CPU values, not serialized descriptor bits.
        let values = [min, min + 1, -3, -2, -1, 0, 1, 2, max - 1, max];
        let mut ctx = CudaSession::create_execution_ctx(&cuda_session())?;
        for literal in [min, min + 1, -2, -1, 0, 1, max - 1, max] {
            macro_rules! narrow_input {
                ($T:ty) => {
                    (
                        PrimitiveArray::from_iter(values.map(|value| {
                            <$T>::try_from(value).vortex_expect("test value fits signed type")
                        })),
                        PValue::from(
                            <$T>::try_from(literal).vortex_expect("test literal fits signed type"),
                        ),
                    )
                };
            }
            let (input, scalar) = match ptype {
                PType::I8 => narrow_input!(i8),
                PType::I16 => narrow_input!(i16),
                PType::I32 => narrow_input!(i32),
                PType::I64 => (PrimitiveArray::from_iter(values), PValue::from(literal)),
                _ => unreachable!("signed integer cases only"),
            };
            for operator in [
                CompareOperator::Eq,
                CompareOperator::NotEq,
                CompareOperator::Gt,
                CompareOperator::Gte,
                CompareOperator::Lt,
                CompareOperator::Lte,
            ] {
                let expected = values
                    .iter()
                    .enumerate()
                    .fold(0u32, |word, (index, &value)| {
                        let keep = match operator {
                            CompareOperator::Eq => value == literal,
                            CompareOperator::NotEq => value != literal,
                            CompareOperator::Gt => value > literal,
                            CompareOperator::Gte => value >= literal,
                            CompareOperator::Lt => value < literal,
                            CompareOperator::Lte => value <= literal,
                        };
                        word | (u32::from(keep) << index)
                    });
                let comparison = FieldComparison {
                    index: 0,
                    operator,
                    literal: scalar,
                };
                let mask = test_selection_mask(
                    vec![(input.as_view(), &comparison)],
                    values.len(),
                    typed_mask,
                    &mut ctx,
                )
                .await?;
                let words = ctx
                    .stream()
                    .clone_dtoh(&mask.words.cuda_view::<u32>()?)
                    .map_err(|error| vortex_err!("Failed to read selection mask: {error}"))?;
                assert_eq!(
                    words,
                    vec![expected],
                    "{ptype} {operator:?} {literal}, typed={typed_mask}"
                );
                assert_eq!(
                    mask.selected,
                    expected.count_ones() as usize,
                    "{ptype} {operator:?} {literal}, typed={typed_mask}"
                );
            }
        }
        Ok(())
    }

    #[rstest]
    #[crate::test]
    async fn test_single_group_comparison_counts(
        #[values(false, true)] typed_mask: bool,
    ) -> VortexResult<()> {
        let values = PrimitiveArray::from_iter(0i32..4099);
        let len = values.len();
        let mut ctx = CudaSession::create_execution_ctx(&cuda_session())?;
        for count in 1..=FILTER_MAX_PREDICATES {
            let comparisons: Vec<_> = (0..count)
                .map(|index| {
                    let (operator, literal) = match index % 3 {
                        0 => (CompareOperator::Gte, 123i32),
                        1 => (CompareOperator::Lt, 333i32),
                        _ => (CompareOperator::NotEq, 200i32),
                    };
                    FieldComparison {
                        index: 0,
                        operator,
                        literal: PValue::from(literal),
                    }
                })
                .collect();
            let inputs = comparisons
                .iter()
                .map(|comparison| (values.as_view(), comparison))
                .collect();
            let mask = test_selection_mask(inputs, len, typed_mask, &mut ctx).await?;
            let mut expected = vec![0u32; len.div_ceil(32)];
            for row in 0..len {
                if row >= 123 && (count < 2 || row < 333) && (count < 3 || row != 200) {
                    expected[row / 32] |= 1u32 << (row % 32);
                }
            }
            let words = ctx
                .stream()
                .clone_dtoh(&mask.words.cuda_view::<u32>()?)
                .map_err(|error| vortex_err!("Failed to read selection mask: {error}"))?;
            assert_eq!(words, expected, "comparison count {count}");
            assert_eq!(
                mask.selected,
                expected
                    .iter()
                    .map(|word| word.count_ones() as usize)
                    .sum::<usize>()
            );
        }
        Ok(())
    }

    /// Mixed-field conjunctions use the per-comparison fallback even at typed capacity.
    #[rstest]
    #[case(SelectionImpl::Generic)]
    #[case(SelectionImpl::Typed)]
    #[crate::test]
    async fn test_repeated_fields_at_predicate_capacity(
        #[case] implementation: SelectionImpl,
    ) -> VortexResult<()> {
        let batch = batch()?;
        let expected = batch.slice(123..333)?;
        let filter = and(
            and(gt_eq(col("y"), lit(0.0f64)), gt_eq(col("x"), lit(123i32))),
            and(lt(col("y"), lit(200.0f64)), lt(col("x"), lit(333i32))),
        );
        let comparisons = field_comparisons(&filter, batch.dtype(), batch.dtype())?;
        let comparisons: Vec<_> = comparisons
            .into_iter()
            .cycle()
            .take(FILTER_MAX_PREDICATES)
            .collect();
        let mut ctx = CudaSession::create_execution_ctx(&cuda_session())?;
        let selected = select_batch(batch, &comparisons, implementation, &mut ctx).await?;
        let selected = selected
            .execute_cuda(&mut ctx)
            .await?
            .into_host(ctx.execution_ctx())
            .await?
            .into_array();
        let mut ctx = array_session().create_execution_ctx();
        assert_arrays_eq!(selected, expected, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[case(PType::F32)]
    #[case(PType::F64)]
    #[crate::test]
    async fn test_float_mask_all_operators_and_ambiguous_literals(
        #[case] ptype: PType,
        #[values(false, true)] typed_mask: bool,
    ) -> VortexResult<()> {
        // All values (including the subnormal) are represented exactly by both float widths.
        let values = [
            -f64::NAN,
            f64::NAN,
            -0.0,
            0.0,
            f64::NEG_INFINITY,
            f64::INFINITY,
            -2.0,
            -1.0,
            1.0,
            2.0,
            f64::from(f32::from_bits(1)),
        ];
        let mut ctx = CudaSession::create_execution_ctx(&cuda_session())?;
        for literal in [
            -f64::NAN,
            f64::NAN,
            -0.0,
            0.0,
            -1.0,
            1.0,
            f64::NEG_INFINITY,
            f64::INFINITY,
        ] {
            for operator in [
                CompareOperator::Eq,
                CompareOperator::NotEq,
                CompareOperator::Gt,
                CompareOperator::Gte,
                CompareOperator::Lt,
                CompareOperator::Lte,
            ] {
                let expected = values
                    .iter()
                    .enumerate()
                    .fold(0u32, |mask, (index, &value)| {
                        let ambiguous =
                            value.is_nan() || literal.is_nan() || (value == 0.0 && literal == 0.0);
                        let keep = ambiguous
                            || match operator {
                                CompareOperator::Eq => value == literal,
                                CompareOperator::NotEq => value != literal,
                                CompareOperator::Gt => value > literal,
                                CompareOperator::Gte => value >= literal,
                                CompareOperator::Lt => value < literal,
                                CompareOperator::Lte => value <= literal,
                            };
                        mask | (u32::from(keep) << index)
                    });
                #[allow(
                    clippy::cast_possible_truncation,
                    reason = "Finite test values are exactly representable in f32"
                )]
                let (input, literal) = match ptype {
                    PType::F32 => (
                        PrimitiveArray::from_iter(values.map(|value| value as f32)),
                        PValue::from(literal as f32),
                    ),
                    PType::F64 => (PrimitiveArray::from_iter(values), PValue::from(literal)),
                    _ => unreachable!("float cases only"),
                };
                let comparison = FieldComparison {
                    index: 0,
                    operator,
                    literal,
                };
                let mask = test_selection_mask(
                    vec![(input.as_view(), &comparison)],
                    values.len(),
                    typed_mask,
                    &mut ctx,
                )
                .await?;
                let words = ctx
                    .stream()
                    .clone_dtoh(&mask.words.cuda_view::<u32>()?)
                    .map_err(|error| vortex_err!("Failed to read selection mask: {error}"))?;
                assert_eq!(words, vec![expected]);
                assert_eq!(mask.selected, expected.count_ones() as usize);

                // The standalone baseline must obey the same contract, including tails.
                let mut baseline = ctx.device_alloc::<u32>(1)?;
                launch_compare(
                    &Canonical::Primitive(input),
                    &comparison,
                    &mut baseline,
                    false,
                    &mut ctx,
                )
                .await?;
                let words = ctx.stream().clone_dtoh(&baseline).map_err(|error| {
                    vortex_err!("Failed to read baseline selection mask: {error}")
                })?;
                assert_eq!(words, vec![expected]);
            }
        }
        Ok(())
    }

    #[rstest]
    #[crate::test]
    async fn test_device_slice_offsets_and_cross_stream_lifetimes(
        #[values(SelectionImpl::Generic, SelectionImpl::Typed)] implementation: SelectionImpl,
    ) -> VortexResult<()> {
        let session = cuda_session();
        let producer = CudaSession::create_execution_ctx(&session)?;
        let x = producer
            .copy_to_device((0i32..5003).collect::<Vec<_>>())?
            .await?;
        let y = producer
            .copy_to_device((0u64..5003).map(|row| u64::MAX - row).collect::<Vec<_>>())?
            .await?;
        let x = PrimitiveArray::from_buffer_handle(
            x.slice(3 * 4..5003 * 4),
            PType::I32,
            Validity::AllValid,
        )
        .into_array();
        let y = PrimitiveArray::from_buffer_handle(
            y.slice(3 * 8..5003 * 8),
            PType::U64,
            Validity::NonNullable,
        )
        .into_array();
        let batch = StructArray::from_fields(&[("x", x), ("y", y)])?.into_array();
        let filter = and(gt_eq(col("x"), lit(1000i32)), lt(col("x"), lit(1100i32)));
        let comparisons = field_comparisons(&filter, batch.dtype(), batch.dtype())?;
        let mut consumer = CudaSession::create_execution_ctx(&session)?;
        let selected = select_batch(batch, &comparisons, implementation, &mut consumer).await?;
        // Drop both contexts and all source handles before a third stream consumes outputs.
        drop((producer, consumer));
        let mut reader = CudaSession::create_execution_ctx(&session)?;
        let selected = selected
            .execute_cuda(&mut reader)
            .await?
            .into_host(reader.execution_ctx())
            .await?
            .into_array();
        let expected = StructArray::from_fields(&[
            (
                "x",
                PrimitiveArray::from_option_iter((1000i32..1100).map(Some)).into_array(),
            ),
            (
                "y",
                PrimitiveArray::from_iter((1000u64..1100).map(|row| u64::MAX - row)).into_array(),
            ),
        ])?
        .into_array();
        let mut ctx = array_session().create_execution_ctx();
        assert_arrays_eq!(selected, expected, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[crate::test]
    async fn test_nullable_columns_keep_whole_batch(
        #[values(SelectionImpl::Generic, SelectionImpl::Typed)] implementation: SelectionImpl,
    ) -> VortexResult<()> {
        let x = PrimitiveArray::from_iter(0i32..100).into_array();
        let y = PrimitiveArray::from_option_iter((0i32..100).map(|row| (row != 50).then_some(row)))
            .into_array();
        let batch = StructArray::from_fields(&[("x", x), ("y", y)])?.into_array();
        for filter in [lt(col("x"), lit(5i32)), lt(col("y"), lit(5i32))] {
            let selected = select_with_impl(batch.clone(), filter, implementation).await?;
            let mut ctx = array_session().create_execution_ctx();
            assert_arrays_eq!(selected, batch, &mut ctx);
        }
        Ok(())
    }

    #[rstest]
    #[case(SelectionImpl::Generic)]
    #[case(SelectionImpl::Typed)]
    #[crate::test]
    async fn test_float_ambiguities_preserve_cpu_superset(
        #[case] implementation: SelectionImpl,
    ) -> VortexResult<()> {
        let values = PrimitiveArray::from_iter([f64::NAN, -0.0, 0.0, -1.0, -2.0, -3.0, -4.0, -5.0]);
        let batch = StructArray::from_fields(&[("x", values.into_array())])?.into_array();
        // Vortex orders NaN above finite values and +0 above -0; IEEE consumers differ.
        // Retaining all three ambiguous rows is safe for either consumer.
        let expected = batch.slice(0..3)?;
        let selected = select_with_impl(batch, gt(col("x"), lit(-0.0f64)), implementation).await?;
        let mut ctx = array_session().create_execution_ctx();
        assert_arrays_eq!(selected, expected, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[crate::test]
    async fn test_predicate_capacity_falls_back_to_cub(
        #[values(SelectionImpl::Generic, SelectionImpl::Typed)] implementation: SelectionImpl,
    ) -> VortexResult<()> {
        let batch = batch()?;
        let expected = batch.slice(0..100)?;
        let comparison =
            field_comparisons(&lt(col("x"), lit(100i32)), batch.dtype(), batch.dtype())?;
        let comparisons = vec![comparison[0].clone(); FILTER_MAX_PREDICATES + 1];
        let mut ctx = CudaSession::create_execution_ctx(&cuda_session())?;
        let selected = select_batch(batch, &comparisons, implementation, &mut ctx).await?;
        let selected = selected
            .execute_cuda(&mut ctx)
            .await?
            .into_host(ctx.execution_ctx())
            .await?
            .into_array();
        let mut ctx = array_session().create_execution_ctx();
        assert_arrays_eq!(selected, expected, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[crate::test]
    async fn test_missing_and_unsupported_conjuncts_preserve_superset(
        #[values(SelectionImpl::Generic, SelectionImpl::Typed)] implementation: SelectionImpl,
    ) -> VortexResult<()> {
        let batch = batch()?;
        let scope = DType::struct_(
            ["not_projected", "x"]
                .map(|name| (name, DType::Primitive(PType::I32, Nullability::NonNullable))),
            Nullability::NonNullable,
        );
        let filter = and(
            lt(col("not_projected"), lit(0i32)),
            and(
                lt(col("x"), lit(100i32)),
                or(lt(col("x"), lit(1i32)), gt(col("x"), lit(4998i32))),
            ),
        );
        let comparisons = field_comparisons(&filter, &scope, batch.dtype())?;
        assert_eq!(comparisons.len(), 1);
        let expected = batch.slice(0..100)?;
        let mut ctx = CudaSession::create_execution_ctx(&cuda_session())?;
        let selected = select_batch(batch, &comparisons, implementation, &mut ctx).await?;
        let selected = selected
            .execute_cuda(&mut ctx)
            .await?
            .into_host(ctx.execution_ctx())
            .await?
            .into_array();
        let mut ctx = array_session().create_execution_ctx();
        assert_arrays_eq!(selected, expected, &mut ctx);
        Ok(())
    }
}
