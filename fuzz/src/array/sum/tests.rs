// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::iter;

use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::fns::sum_v2::sum_v2;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::DecimalType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::i256;
use vortex_array::expr::stats::Precision;
use vortex_array::expr::stats::Stat;
use vortex_array::match_each_decimal_value_type;
use vortex_array::scalar::DecimalValue;
use vortex_array::scalar::Scalar;
use vortex_array::scalar::ScalarValue;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_buffer::buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use super::sum_canonical_array;
use crate::Action;
use crate::ExpectedValue;
use crate::FuzzArrayAction;
use crate::SESSION;
use crate::run_fuzz_action;

#[test]
fn test_sum_ignores_cached_statistics() -> VortexResult<()> {
    let array = PrimitiveArray::from_iter([1i64, 2, 3]);
    array
        .as_ref()
        .statistics()
        .set(Stat::Sum, Precision::Exact(ScalarValue::from(999i64)));
    assert_eq!(
        sum_canonical_array(&array.into_array(), &mut SESSION.create_execution_ctx())?,
        Some(Scalar::from(6i64))
    );
    Ok(())
}

#[rstest]
#[case::i8(buffer![i8::MAX, i8::MAX, -1].into_array(), 253i64.into())]
#[case::i16(buffer![i16::MAX, i16::MAX, -1].into_array(), 65_533i64.into())]
#[case::i32(buffer![i32::MAX, i32::MAX, -1].into_array(), 4_294_967_293i64.into())]
#[case::i64(buffer![1i64, 2, 3].into_array(), 6i64.into())]
#[case::u8(buffer![u8::MAX, u8::MAX].into_array(), 510u64.into())]
#[case::u16(buffer![u16::MAX, u16::MAX].into_array(), 131_070u64.into())]
#[case::u32(buffer![u32::MAX, u32::MAX].into_array(), 8_589_934_590u64.into())]
#[case::u64(buffer![u64::MAX, 0].into_array(), u64::MAX.into())]
#[case::unsigned_overflow(buffer![u64::MAX, 1].into_array(), Scalar::from(None::<u64>))]
#[case::unsigned_nulls(
    PrimitiveArray::new(buffer![u64::MAX, u64::MAX], Validity::from_iter([true, false])).into_array(),
    u64::MAX.into()
)]
#[case::unsigned_all_null(PrimitiveArray::from_option_iter([None::<u64>, None]).into_array(), Scalar::from(None::<u64>))]
#[case::unsigned_empty(Buffer::<u64>::empty().into_array(), Scalar::from(None::<u64>))]
#[case::signed_empty(Buffer::<i32>::empty().into_array(), Scalar::from(None::<i64>))]
#[case::bool(BoolArray::from_iter([true, false, true]).into_array(), 2u64.into())]
#[case::bool_nulls(BoolArray::from_iter([Some(true), None, Some(false)]).into_array(), 1u64.into())]
#[case::bool_all_null(BoolArray::from_iter([None::<bool>, None]).into_array(), Scalar::from(None::<u64>))]
#[case::bool_empty(BoolArray::from_iter([] as [bool; 0]).into_array(), Scalar::from(None::<u64>))]
fn test_sum_integer_and_bool_values(
    #[case] array: ArrayRef,
    #[case] expected: Scalar,
) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let result = sum_canonical_array(
        &array.execute::<Canonical>(&mut ctx)?.into_array(),
        &mut ctx,
    )?
    .ok_or_else(|| vortex_err!("expected an unambiguous sum"))?;
    assert_eq!(result.dtype(), &expected.dtype().as_nullable());
    assert_eq!(result, expected);
    Ok(())
}

#[rstest]
#[case(DecimalType::I8)]
#[case(DecimalType::I16)]
#[case(DecimalType::I32)]
#[case(DecimalType::I64)]
#[case(DecimalType::I128)]
#[case(DecimalType::I256)]
fn test_sum_decimal_storage(#[case] values_type: DecimalType) -> VortexResult<()> {
    let array = match_each_decimal_value_type!(values_type, |D| {
        let value = DecimalValue::I8(99)
            .cast::<D>()
            .ok_or_else(|| vortex_err!("99 fits in every decimal storage type"))?;
        DecimalArray::new(
            buffer![value, value, -value],
            DecimalDType::new(2, 0),
            Validity::NonNullable,
        )
    });
    let result = sum_canonical_array(&array.into_array(), &mut SESSION.create_execution_ctx())?
        .ok_or_else(|| vortex_err!("expected an unambiguous sum"))?;
    assert_eq!(
        result.dtype(),
        &DType::Decimal(DecimalDType::new(12, 0), Nullability::Nullable)
    );
    assert_eq!(
        result.as_decimal().decimal_value(),
        Some(DecimalValue::I64(99))
    );
    Ok(())
}

#[test]
fn test_sum_decimal_widens_beyond_i128() -> VortexResult<()> {
    let value = 10i128.pow(38) - 1;
    let array = DecimalArray::new(
        buffer![value, value],
        DecimalDType::new(38, 2),
        Validity::NonNullable,
    );
    assert_eq!(
        sum_canonical_array(&array.into_array(), &mut SESSION.create_execution_ctx())?,
        Some(Scalar::decimal(
            DecimalValue::I256(i256::from_i128(value) * i256::from_i128(2)),
            DecimalDType::new(48, 2),
            Nullability::Nullable,
        ))
    );
    Ok(())
}

#[test]
fn test_sum_decimal_cancellation_across_groups() -> VortexResult<()> {
    let decimal_dtype = DecimalDType::new(76, -76);
    let six_e75 = i256::from_i128(10).wrapping_pow(75) * i256::from_i128(6);

    for value in [six_e75, -six_e75] {
        let first = DecimalArray::new(buffer![value, value], decimal_dtype, Validity::NonNullable)
            .into_array();
        let last =
            DecimalArray::new(buffer![-value], decimal_dtype, Validity::NonNullable).into_array();
        let dtype = first.dtype().clone();
        let chunked = ChunkedArray::try_new(vec![first, last], dtype)?.into_array();
        let mut ctx = SESSION.create_execution_ctx();

        // The first chunk exceeds precision 76, but the final sum fits after cancellation.
        let expected = Scalar::decimal(
            DecimalValue::I256(value),
            decimal_dtype,
            Nullability::Nullable,
        );
        let canonical = chunked.clone().execute::<Canonical>(&mut ctx)?.into_array();
        for array in [chunked, canonical] {
            assert_eq!(sum_v2(&array, &mut ctx)?, expected);
            assert_eq!(
                sum_canonical_array(&array, &mut ctx)?,
                Some(expected.clone())
            );
        }
    }
    Ok(())
}

#[test]
fn test_sum_decimal_precision_boundary_and_nulls() -> VortexResult<()> {
    let dtype = DecimalDType::new(76, 0);
    let max = i256::from_i128(10).wrapping_pow(76) - i256::ONE;
    let mut ctx = SESSION.create_execution_ctx();
    for validity in [
        Validity::from_iter([true, false, true]),
        Validity::AllInvalid,
    ] {
        let array = DecimalArray::new(buffer![max, max, -max], dtype, validity);
        let expected = if matches!(array.as_ref().validity()?, Validity::AllInvalid) {
            Scalar::null(DType::Decimal(dtype, Nullability::Nullable))
        } else {
            Scalar::decimal(DecimalValue::I256(i256::ZERO), dtype, Nullability::Nullable)
        };
        assert_eq!(
            sum_canonical_array(&array.into_array(), &mut ctx)?,
            Some(expected)
        );
    }
    Ok(())
}

#[test]
fn test_sum_decimal_keeps_definite_overflow() -> VortexResult<()> {
    let dtype = DecimalDType::new(76, 0);
    let six_e75 = i256::from_i128(10).wrapping_pow(75) * i256::from_i128(6);
    let mut ctx = SESSION.create_execution_ctx();
    for value in [six_e75, -six_e75] {
        let array = DecimalArray::new(buffer![value, value], dtype, Validity::NonNullable);
        assert_eq!(
            sum_canonical_array(&array.into_array(), &mut ctx)?,
            Some(Scalar::null(DType::Decimal(dtype, Nullability::Nullable)))
        );
    }
    Ok(())
}

#[test]
fn test_sum_decimal_native_overflow_is_absorbing() -> VortexResult<()> {
    let dtype = DecimalDType::new(76, 0);
    let six_e75 = i256::from_i128(10).wrapping_pow(75) * i256::from_i128(6);
    let mut ctx = SESSION.create_execution_ctx();
    for value in [six_e75, -six_e75] {
        let values = iter::repeat_n(value, 10)
            .chain(iter::repeat_n(-value, 10))
            .collect::<Buffer<_>>();
        let array = DecimalArray::new(values, dtype, Validity::NonNullable);
        let expected = Scalar::null(DType::Decimal(dtype, Nullability::Nullable));
        let array = array.into_array();
        assert_eq!(
            sum_canonical_array(&array, &mut ctx)?,
            Some(expected.clone())
        );
        assert_eq!(sum_v2(&array, &mut ctx)?, expected);
    }
    Ok(())
}

#[test]
fn test_sum_decimal_uses_widened_precision() -> VortexResult<()> {
    let array = DecimalArray::new(
        buffer![99i8, 99, -99],
        DecimalDType::new(2, 0),
        Validity::NonNullable,
    );
    assert_eq!(
        sum_canonical_array(&array.into_array(), &mut SESSION.create_execution_ctx())?,
        Some(Scalar::decimal(
            DecimalValue::I64(99),
            DecimalDType::new(12, 0),
            Nullability::Nullable
        ))
    );
    Ok(())
}

#[test]
fn test_sum_signed_overflow_and_cancellation() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    for (values, expected) in [
        (vec![Some(i64::MAX), Some(1), Some(-1)], Some(None)),
        (vec![Some(i64::MIN), Some(-1), Some(1)], Some(None)),
        (
            vec![Some(i64::MAX), Some(-1), Some(1)],
            Some(Some(i64::MAX)),
        ),
        (vec![Some(i64::MAX), Some(i64::MIN)], Some(Some(-1i64))),
        (vec![Some(i64::MAX), Some(1)], Some(None)),
        (vec![Some(i64::MIN), Some(-1)], Some(None)),
        (vec![Some(i64::MAX), None, Some(i64::MIN)], Some(Some(-1))),
        (vec![None, None], Some(None)),
        (vec![], Some(None)),
    ] {
        let array = PrimitiveArray::from_option_iter(values);
        assert_eq!(
            sum_canonical_array(&array.into_array(), &mut ctx)?,
            expected.map(Scalar::from)
        );
    }
    Ok(())
}

fn decimal_chunks(groups: &[&[i8]]) -> VortexResult<ArrayRef> {
    let unit = i256::from_i128(10).wrapping_pow(75);
    let dtype = DecimalDType::new(76, -76);
    ChunkedArray::try_new(
        groups.iter().map(|values| {
            DecimalArray::new(
                values
                    .iter()
                    .map(|&value| unit * i256::from_i128(i128::from(value)))
                    .collect::<Buffer<_>>(),
                dtype,
                Validity::NonNullable,
            )
            .into_array()
        }),
        DType::Decimal(dtype, Nullability::NonNullable),
    )
    .map(IntoArray::into_array)
}

#[rstest]
#[case::cancellation_across_groups(decimal_chunks(&[&[6, 6], &[-6]]), Some(6))]
#[case::cancellation_across_three_groups(decimal_chunks(&[&[6], &[6], &[-6]]), Some(6))]
#[case::negative_cancellation(decimal_chunks(&[&[-6, -6], &[6]]), Some(-6))]
#[case::shared_running_total(decimal_chunks(&[&[-6], &[6, 6]]), Some(6))]
#[case::cancellation_within_group(decimal_chunks(&[&[6, 6, -6]]), Some(6))]
#[case::empty_groups(decimal_chunks(&[&[], &[6], &[], &[-6], &[]]), Some(0))]
#[case::final_overflow(decimal_chunks(&[&[6], &[6]]), None)]
#[case::all_empty(decimal_chunks(&[&[], &[]]), None)]
fn test_sum_decimal_ignores_group_boundaries(
    #[case] array: VortexResult<ArrayRef>,
    #[case] expected: Option<i8>,
) -> VortexResult<()> {
    let array = array?;
    let mut ctx = SESSION.create_execution_ctx();
    let expected = match expected {
        Some(value) => Scalar::decimal(
            DecimalValue::I256(
                i256::from_i128(10).wrapping_pow(75) * i256::from_i128(i128::from(value)),
            ),
            DecimalDType::new(76, -76),
            Nullability::Nullable,
        ),
        None => Scalar::null(array.dtype().as_nullable()),
    };
    assert_eq!(
        sum_canonical_array(&array, &mut ctx)?,
        Some(expected.clone())
    );
    assert_eq!(sum_v2(&array, &mut ctx)?, expected);
    Ok(())
}

#[test]
fn test_sum_nested_group_cancellation() -> VortexResult<()> {
    let inner = decimal_chunks(&[&[6, 6], &[-6]])?;
    let array = ChunkedArray::try_new(
        vec![decimal_chunks(&[&[-6]])?, inner.clone()],
        inner.dtype().clone(),
    )?
    .into_array();
    let mut ctx = SESSION.create_execution_ctx();
    let expected = Scalar::decimal(
        DecimalValue::I256(i256::ZERO),
        DecimalDType::new(76, -76),
        Nullability::Nullable,
    );
    assert_eq!(
        sum_canonical_array(&array, &mut ctx)?,
        Some(expected.clone())
    );
    assert_eq!(sum_v2(&array, &mut ctx)?, expected);
    Ok(())
}

#[test]
fn test_sum_cached_statistics_do_not_change_native_overflow() -> VortexResult<()> {
    let array = buffer![i64::MAX, 1, -1].into_array();
    array
        .statistics()
        .set(Stat::Sum, Precision::Exact(ScalarValue::from(i64::MAX)));
    let mut ctx = SESSION.create_execution_ctx();
    assert_eq!(
        sum_canonical_array(&array, &mut ctx)?,
        Some(Scalar::from(None::<i64>))
    );
    Ok(())
}

#[test]
fn test_sum_cached_statistics_do_not_change_groups() -> VortexResult<()> {
    let array = decimal_chunks(&[&[6, 6], &[-6]])?;
    array.statistics().set(
        Stat::Sum,
        Precision::Exact(ScalarValue::from(DecimalValue::I256(
            i256::from_i128(10).wrapping_pow(75) * i256::from_i128(6),
        ))),
    );
    let mut ctx = SESSION.create_execution_ctx();
    assert_eq!(
        sum_canonical_array(&array, &mut ctx)?,
        Some(Scalar::decimal(
            DecimalValue::I256(i256::from_i128(10).wrapping_pow(75) * i256::from_i128(6)),
            DecimalDType::new(76, -76),
            Nullability::Nullable
        ))
    );
    Ok(())
}

#[rstest]
#[case::chunked(false, Some(6))]
#[case::canonical(true, Some(6))]
fn test_sum_action_stores_reference_scalar(
    #[case] canonical: bool,
    #[case] expected_sum: Option<i8>,
) -> VortexResult<()> {
    let mut array = decimal_chunks(&[&[6, 6], &[-6]])?;
    let mut ctx = SESSION.create_execution_ctx();
    if canonical {
        array = array.execute::<Canonical>(&mut ctx)?.into_array();
    }
    let expected_sum = match expected_sum {
        Some(value) => Scalar::decimal(
            DecimalValue::I256(
                i256::from_i128(10).wrapping_pow(75) * i256::from_i128(i128::from(value)),
            ),
            DecimalDType::new(76, -76),
            Nullability::Nullable,
        ),
        None => Scalar::null(array.dtype().as_nullable()),
    };
    let reference_sum = sum_canonical_array(&array, &mut ctx)?
        .ok_or_else(|| vortex_err!("expected a decimal sum"))?;
    assert_eq!(reference_sum, expected_sum);
    let fuzz_action = FuzzArrayAction {
        array,
        actions: vec![(Action::Sum, ExpectedValue::Scalar(reference_sum))],
    };
    assert!(run_fuzz_action(fuzz_action).map_err(|error| vortex_err!("{error}"))?);
    Ok(())
}
