// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use rstest::rstest;
use vortex_error::VortexResult;

use super::*;
use crate::AnyColumnar;
use crate::Canonical;
use crate::VortexSessionExecute;
use crate::array_session;
use crate::arrays::Bool;
use crate::arrays::BoolArray;
use crate::arrays::Primitive;
use crate::arrays::PrimitiveArray;
use crate::arrays::SliceArray;
use crate::assert_arrays_eq;
use crate::dtype::PType;

#[derive(Clone, Copy)]
enum LazyChild {
    ArraySelector,
    RowSelector,
    ThirdValue,
}

#[rstest]
#[case::array_selector(LazyChild::ArraySelector)]
#[case::row_selector(LazyChild::RowSelector)]
#[case::third_value(LazyChild::ThirdValue)]
fn executes_lazy_children(
    #[case] lazy_child: LazyChild,
    #[values(false, true)] boolean: bool,
) -> VortexResult<()> {
    let (mut values, expected) = if boolean {
        (
            vec![
                BoolArray::from_iter([true, false]).into_array(),
                BoolArray::from_iter([false, true]).into_array(),
                BoolArray::from_iter([Some(false), None]).into_array(),
            ],
            BoolArray::from_iter([None, Some(true), Some(true), Some(false), Some(false)])
                .into_array(),
        )
    } else {
        (
            vec![
                PrimitiveArray::from_iter([10i32, 11]).into_array(),
                PrimitiveArray::from_iter([20i32, 21]).into_array(),
                PrimitiveArray::from_option_iter([Some(30i32), None]).into_array(),
            ],
            PrimitiveArray::from_option_iter([None, Some(10i32), Some(21), Some(30), Some(11)])
                .into_array(),
        )
    };
    let mut array_indices = PrimitiveArray::from_iter([2u8, 0, 1, 2, 0]).into_array();
    let mut row_indices = PrimitiveArray::from_iter([1u32, 0, 1, 0, 1]).into_array();
    let child = match lazy_child {
        LazyChild::ArraySelector => &mut array_indices,
        LazyChild::RowSelector => &mut row_indices,
        LazyChild::ThirdValue => &mut values[2],
    };
    *child = SliceArray::try_new(child.clone(), 0..child.len())?.into_array();

    assert_eq!(
        array_indices.is::<Primitive>(),
        !matches!(lazy_child, LazyChild::ArraySelector)
    );
    assert_eq!(
        row_indices.is::<Primitive>(),
        !matches!(lazy_child, LazyChild::RowSelector)
    );
    for (i, value) in values.iter().enumerate() {
        let matches = if boolean {
            value.is::<Bool>()
        } else {
            value.is::<AnyColumnar>()
        };
        assert_eq!(
            matches,
            !(matches!(lazy_child, LazyChild::ThirdValue) && i == 2)
        );
    }

    let interleaved = InterleaveArray::try_new(values, array_indices, row_indices)?.into_array();
    let mut ctx = array_session().create_execution_ctx();
    assert_arrays_eq!(interleaved, expected, &mut ctx);
    Ok(())
}

/// Reference (oracle) implementation of the interleave spec, used only to validate the optimized
/// [execute](super::execute) path. It is intentionally simple and slow: it pulls each output
/// element one [`Scalar`] at a time via [`ArrayRef::execute_scalar`] and never touches raw bits.
///
/// This is deliberately *not* wired into the array execution path — it exists purely as a
/// trustworthy comparison point in tests.
fn interleave_reference(
    values: &[ArrayRef],
    array_indices: &ArrayRef,
    row_indices: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrayRef> {
    let len = array_indices.len();
    let nullable = values.iter().any(|v| v.dtype().is_nullable());
    let mut out: Vec<Option<bool>> = Vec::with_capacity(len);

    for i in 0..len {
        let j = array_indices
            .execute_scalar(i, ctx)?
            .as_primitive()
            .as_::<usize>()
            .vortex_expect("array_indices is non-nullable");
        let row = row_indices
            .execute_scalar(i, ctx)?
            .as_primitive()
            .as_::<usize>()
            .vortex_expect("row_indices is non-nullable");
        out.push(values[j].execute_scalar(row, ctx)?.as_bool().value());
    }

    Ok(if nullable {
        BoolArray::from_iter(out).into_array()
    } else {
        BoolArray::from_iter(
            out.into_iter()
                .map(|v| v.vortex_expect("non-nullable value produced a null")),
        )
        .into_array()
    })
}

/// Builds the compact value arrays and the unsigned `(array_indices, row_indices)` selectors for
/// a gather described by per-output `(array_index, row_index)` pairs over `branches`.
fn build(
    branches: &[&[Option<bool>]],
    indices: &[(usize, usize)],
) -> (Vec<ArrayRef>, ArrayRef, ArrayRef) {
    let nullable = branches.iter().flat_map(|b| b.iter()).any(Option::is_none);
    let to_value = |vals: &[Option<bool>]| -> ArrayRef {
        if nullable {
            BoolArray::from_iter(vals.iter().copied()).into_array()
        } else {
            BoolArray::from_iter(
                vals.iter()
                    .map(|v| v.vortex_expect("non-nullable value produced a null")),
            )
            .into_array()
        }
    };

    let values = branches.iter().map(|b| to_value(b)).collect();
    let array_indices = PrimitiveArray::from_iter(
        indices
            .iter()
            .map(|&(a, _)| u32::try_from(a).vortex_expect("array index fits in u32")),
    )
    .into_array();
    let row_indices = PrimitiveArray::from_iter(
        indices
            .iter()
            .map(|&(_, r)| u32::try_from(r).vortex_expect("row index fits in u32")),
    )
    .into_array();
    (values, array_indices, row_indices)
}

/// Asserts that the optimized execute path and the reference implementation agree, exercising
/// `InterleaveArray` construction, `execute`, `scalar_at`, and `validity` (via
/// `assert_arrays_eq`).
fn check(branches: &[&[Option<bool>]], indices: &[(usize, usize)]) -> VortexResult<()> {
    let (values, array_indices, row_indices) = build(branches, indices);

    let interleaved =
        InterleaveArray::try_new(values.clone(), array_indices.clone(), row_indices.clone())?
            .into_array();

    let mut ctx = array_session().create_execution_ctx();
    let reference = interleave_reference(&values, &array_indices, &row_indices, &mut ctx)?;

    assert_arrays_eq!(interleaved, reference, &mut ctx);
    Ok(())
}

#[test]
fn interleave_reorders_and_repeats() -> VortexResult<()> {
    // Random access: rows are pulled out of order and branch 0 row 0 is repeated.
    check(
        &[&[Some(true), Some(false)], &[Some(false), Some(true)]],
        &[(0, 1), (1, 0), (0, 0), (1, 1), (0, 0)],
    )
}

#[test]
fn interleave_skips_rows() -> VortexResult<()> {
    // Branch 0 row 1 and branch 1 row 0 are never gathered.
    check(
        &[
            &[Some(true), Some(false), Some(true)],
            &[Some(false), Some(true)],
        ],
        &[(0, 0), (1, 1), (0, 2)],
    )
}

#[test]
fn interleave_binary_spans_word_boundary() -> VortexResult<()> {
    // Exercises a two-value gather across more than one 64-bit packing word, with out-of-order
    // routing into both branches so neither buffer is consumed contiguously.
    let branch0: Vec<Option<bool>> = (0..100).map(|i| Some(i % 3 == 0)).collect();
    let branch1: Vec<Option<bool>> = (0..100).map(|i| Some(i % 5 == 0)).collect();
    let indices: Vec<(usize, usize)> = (0..200).map(|i| (i % 2, (i * 7) % 100)).collect();
    check(&[&branch0, &branch1], &indices)
}

#[test]
fn interleave_binary_nullable_spans_word_boundary() -> VortexResult<()> {
    // Same two-value gather, now with nulls so the validity packing is exercised too.
    let branch0: Vec<Option<bool>> = (0..70)
        .map(|i| (i % 4 != 0).then_some(i % 2 == 0))
        .collect();
    let branch1: Vec<Option<bool>> = (0..70)
        .map(|i| (i % 3 != 0).then_some(i % 2 == 1))
        .collect();
    let indices: Vec<(usize, usize)> = (0..150).map(|i| (i % 2, (i * 11) % 70)).collect();
    check(&[&branch0, &branch1], &indices)
}

#[test]
fn interleave_three_values() -> VortexResult<()> {
    // An unsigned `array_indices` routes among three values with full random access.
    check(
        &[
            &[Some(true), Some(false)],
            &[Some(false)],
            &[Some(true), Some(true), Some(false)],
        ],
        &[(2, 1), (0, 0), (1, 0), (2, 2), (0, 1), (2, 0)],
    )
}

#[test]
fn interleave_only_one_branch() -> VortexResult<()> {
    check(
        &[&[Some(true), Some(false), Some(true)], &[Some(false)]],
        &[(0, 2), (0, 0), (0, 1)],
    )
}

#[test]
fn interleave_nullable_with_nulls_in_values() -> VortexResult<()> {
    check(
        &[&[None, Some(true), None], &[Some(false), None]],
        &[(1, 1), (0, 0), (1, 0), (0, 2), (0, 1)],
    )
}

#[test]
fn interleave_empty() -> VortexResult<()> {
    check(&[&[Some(true)], &[Some(false)]], &[])
}

#[test]
fn rejects_boolean_array_indices() {
    let value = BoolArray::from_iter([true, false]).into_array();
    let array_indices = BoolArray::from_iter([true, false]).into_array();
    let row_indices = PrimitiveArray::from_iter([0u32, 1]).into_array();
    let err = InterleaveArray::try_new(vec![value.clone(), value], array_indices, row_indices)
        .err()
        .vortex_expect("expected interleave to reject a boolean array_indices");
    assert!(err.to_string().contains("unsigned integer"), "{err}");
}

#[test]
fn rejects_signed_integer_array_indices() {
    let value = BoolArray::from_iter([true]).into_array();
    let array_indices = PrimitiveArray::from_iter([0i32, 1]).into_array();
    let row_indices = PrimitiveArray::from_iter([0u32, 0]).into_array();
    let err = InterleaveArray::try_new(vec![value.clone(), value], array_indices, row_indices)
        .err()
        .vortex_expect("expected interleave to reject a signed integer array_indices");
    assert!(err.to_string().contains("unsigned integer"), "{err}");
}

#[test]
fn rejects_nullable_row_indices() {
    let value = BoolArray::from_iter([true, false]).into_array();
    let array_indices = PrimitiveArray::from_iter([0u32, 1]).into_array();
    let row_indices = PrimitiveArray::from_option_iter([Some(0u32), Some(1)]).into_array();
    let err = InterleaveArray::try_new(vec![value.clone(), value], array_indices, row_indices)
        .err()
        .vortex_expect("expected interleave to reject nullable row_indices");
    assert!(err.to_string().contains("non-nullable"), "{err}");
}

#[test]
fn rejects_mismatched_selector_lengths() {
    let value = BoolArray::from_iter([true, false]).into_array();
    let array_indices = PrimitiveArray::from_iter([0u32, 1]).into_array();
    let row_indices = PrimitiveArray::from_iter([0u32]).into_array();
    let err = InterleaveArray::try_new(vec![value.clone(), value], array_indices, row_indices)
        .err()
        .vortex_expect("expected interleave to reject mismatched selector lengths");
    assert!(err.to_string().contains("equal length"), "{err}");
}

#[test]
fn execute_rejects_out_of_bounds_array_index() -> VortexResult<()> {
    let value = BoolArray::from_iter([true]).into_array();
    let array_indices = PrimitiveArray::from_iter([2u32]).into_array();
    let row_indices = PrimitiveArray::from_iter([0u32]).into_array();
    let interleaved =
        InterleaveArray::try_new(vec![value.clone(), value], array_indices, row_indices)?
            .into_array();

    let mut ctx = array_session().create_execution_ctx();
    let err = interleaved
        .execute::<Canonical>(&mut ctx)
        .err()
        .vortex_expect("expected execution to reject out-of-bounds array index");
    assert!(
        err.to_string().contains("array index out of bounds"),
        "{err}"
    );
    Ok(())
}

#[test]
fn execute_rejects_out_of_bounds_row_index() -> VortexResult<()> {
    let value = BoolArray::from_iter([true]).into_array();
    let array_indices = PrimitiveArray::from_iter([0u32]).into_array();
    let row_indices = PrimitiveArray::from_iter([1u32]).into_array();
    let interleaved =
        InterleaveArray::try_new(vec![value.clone(), value], array_indices, row_indices)?
            .into_array();

    let mut ctx = array_session().create_execution_ctx();
    let err = interleaved
        .execute::<Canonical>(&mut ctx)
        .err()
        .vortex_expect("expected execution to reject out-of-bounds row index");
    assert!(err.to_string().contains("row index out of bounds"), "{err}");
    Ok(())
}

#[test]
fn executes_primitive_values() -> VortexResult<()> {
    let v0 = PrimitiveArray::from_iter([1.0f64, 2.0]).into_array();
    let v1 = PrimitiveArray::from_option_iter([Some(10.0f64), None]).into_array();
    let array_indices = PrimitiveArray::from_iter([0u8, 1, 0, 1]).into_array();
    let row_indices = PrimitiveArray::from_iter([0u32, 0, 1, 1]).into_array();
    let interleaved =
        InterleaveArray::try_new(vec![v0, v1], array_indices, row_indices)?.into_array();
    let expected =
        PrimitiveArray::from_option_iter([Some(1.0f64), Some(10.0), Some(2.0), None]).into_array();
    let mut ctx = array_session().create_execution_ctx();
    assert_arrays_eq!(interleaved, expected, &mut ctx);
    Ok(())
}

#[test]
fn executes_primitive_constant_values() -> VortexResult<()> {
    let constant = ConstantArray::new(1.0f64, 2).into_array();
    let column = PrimitiveArray::from_iter([10.0f64, 20.0]).into_array();
    let array_indices = PrimitiveArray::from_iter([0u8, 1, 0, 1]).into_array();
    let row_indices = PrimitiveArray::from_iter([0u32, 0, 1, 1]).into_array();
    let interleaved =
        InterleaveArray::try_new(vec![constant, column], array_indices, row_indices)?.into_array();
    let expected = PrimitiveArray::from_iter([1.0f64, 10.0, 1.0, 20.0]).into_array();
    let mut ctx = array_session().create_execution_ctx();

    assert_arrays_eq!(interleaved, expected, &mut ctx);
    Ok(())
}

#[test]
fn executes_null_primitive_constant_values() -> VortexResult<()> {
    let constant = ConstantArray::new(
        Scalar::null(DType::Primitive(PType::F64, Nullability::Nullable)),
        2,
    )
    .into_array();
    let column = PrimitiveArray::from_iter([10.0f64, 20.0]).into_array();
    let array_indices = PrimitiveArray::from_iter([0u8, 1, 0, 1]).into_array();
    let row_indices = PrimitiveArray::from_iter([0u32, 0, 1, 1]).into_array();
    let interleaved =
        InterleaveArray::try_new(vec![constant, column], array_indices, row_indices)?.into_array();
    let expected =
        PrimitiveArray::from_option_iter([None, Some(10.0f64), None, Some(20.0)]).into_array();
    let mut ctx = array_session().create_execution_ctx();

    assert_arrays_eq!(interleaved, expected, &mut ctx);
    Ok(())
}
