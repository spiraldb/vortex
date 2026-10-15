// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Exact row selection for CUDA scans whose filter otherwise only prunes zones.
//!
//! [`scan_cuda_pruned`](crate::scan_cuda_pruned) skips zones a filter rules out, but returns kept
//! zones whole, so a consumer that filters afterwards copies and evaluates every row of a kept
//! zone. [`select_rows`] additionally evaluates the filter's comparisons of a field with a
//! literal on the GPU, and drops the failing rows from each decoded batch before export.

use std::sync::Arc;

use cudarc::driver::CudaSlice;
use cudarc::driver::CudaView;
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

/// One conjunct of a scan filter: a projected field compared with a non-null literal.
#[derive(Debug)]
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
    let batches = stream
        .map(move |batch| {
            let comparisons = Arc::clone(&comparisons);
            let session = session.clone();
            async move {
                let mut ctx = CudaSession::create_execution_ctx(&session)?;
                select_batch(batch?, &comparisons, &mut ctx).await
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
        let mut words = ctx.device_alloc::<u32>(len.div_ceil(32))?;
        for (position, comparison) in comparisons.iter().enumerate() {
            let field = &fields[comparison.index];
            launch_compare(field, comparison, &mut words, position > 0, ctx).await?;
        }
        let mask = BufferHandle::new_device(Arc::new(CudaDeviceBuffer::new(words)));
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
    use vortex::array::expr::gt;
    use vortex::array::expr::gt_eq;
    use vortex::array::expr::lit;
    use vortex::array::expr::lt;
    use vortex::array::expr::or;
    use vortex::array::extension::datetime::TimeUnit;
    use vortex::dtype::Nullability;

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
        let comparisons = field_comparisons(&filter, batch.dtype(), batch.dtype())?;
        let mut ctx = CudaSession::create_execution_ctx(&cuda_session())?;
        let selected = select_batch(batch, &comparisons, &mut ctx).await?;
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
    #[crate::test]
    async fn dates_compare_on_storage() -> VortexResult<()> {
        let days = PrimitiveArray::from_iter(0i32..5000).into_array();
        let date = TemporalArray::new_date(days, TimeUnit::Days).into_array();
        let batch = StructArray::from_fields(&[("d", date)])?.into_array();
        let selected = select(batch, gt_eq(col("d"), lit(4500i32))).await?;
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
}
