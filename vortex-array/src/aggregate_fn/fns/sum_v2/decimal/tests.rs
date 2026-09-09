// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::iter;

use rstest::rstest;
use vortex_buffer::Buffer;
use vortex_buffer::buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use super::super::SumV2;
use super::super::sum_v2;
use super::decimal_partial_scalar;
use super::decimal_partial_value;
use crate::IntoArray;
use crate::VortexSessionExecute;
use crate::aggregate_fn::Accumulator;
use crate::aggregate_fn::DynAccumulator;
use crate::aggregate_fn::DynGroupedAccumulator;
use crate::aggregate_fn::GroupedAccumulator;
use crate::aggregate_fn::NumericalAggregateOpts;
use crate::array_session;
use crate::arrays::ChunkedArray;
use crate::arrays::ConstantArray;
use crate::arrays::DecimalArray;
use crate::arrays::ListViewArray;
use crate::assert_arrays_eq;
use crate::dtype::DType;
use crate::dtype::DecimalDType;
use crate::dtype::DecimalType;
use crate::dtype::Nullability;
use crate::dtype::i256;
use crate::match_each_decimal_value_type;
use crate::scalar::DecimalValue;
use crate::scalar::Scalar;
use crate::validity::Validity;

#[rstest]
fn partial_preserves_native_extremes(
    #[values(11, 18, 19, 38, 39, 76)] precision: u8,
) -> VortexResult<()> {
    let dtype = DecimalDType::new(precision, 0);
    let values_type = DecimalType::smallest_decimal_value_type(&dtype);
    match_each_decimal_value_type!(values_type, |I| {
        for value in [I::MIN, I::MAX] {
            let value = DecimalValue::from(value);
            let partial = decimal_partial_scalar(value, dtype, Nullability::NonNullable);
            assert_eq!(decimal_partial_value(&partial, dtype)?, value);
        }
    });
    Ok(())
}

#[rstest]
fn cancellation_across_batches(
    #[values(1, 2, 7, 10, 21)] batch_size: usize,
    #[values(false, true)] negative: bool,
    #[values(false, true)] nullable: bool,
) -> VortexResult<()> {
    let dtype = DecimalDType::new(76, -76);
    let unit = i256::from_i128(10).wrapping_pow(75);
    let value = unit * i256::from_i128(if negative { -6 } else { 6 });
    // Ten values overflow i256 before cancellation; the final total is one input value.
    let values = iter::repeat_n(value, 10)
        .chain(iter::repeat_n(-value, 9))
        .collect::<Vec<_>>();
    let chunks = values
        .chunks(batch_size)
        .map(|values| {
            let mut values = values.to_vec();
            let validity = if nullable {
                values.push(value);
                Validity::from_iter((0..values.len()).map(|i| i + 1 < values.len()))
            } else {
                Validity::NonNullable
            };
            DecimalArray::new(values.into_iter().collect::<Buffer<_>>(), dtype, validity)
                .into_array()
        })
        .collect::<Vec<_>>();
    let input_dtype = DType::Decimal(
        dtype,
        if nullable {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        },
    );
    let expected = Scalar::decimal(DecimalValue::I256(value), dtype, Nullability::Nullable);
    let mut ctx = array_session().create_execution_ctx();
    let mut accumulator = Accumulator::try_new(
        SumV2,
        NumericalAggregateOpts::default(),
        input_dtype.clone(),
    )?;
    let mut combined = Accumulator::try_new(
        SumV2,
        NumericalAggregateOpts::default(),
        input_dtype.clone(),
    )?;
    for chunk in &chunks {
        accumulator.accumulate(chunk, &mut ctx)?;
        let mut partial = Accumulator::try_new(
            SumV2,
            NumericalAggregateOpts::default(),
            input_dtype.clone(),
        )?;
        partial.accumulate(chunk, &mut ctx)?;
        combined.combine_partials(partial.flush()?)?;
    }
    assert_eq!(accumulator.finish()?, expected);
    assert_eq!(combined.finish()?, expected);
    let chunked = ChunkedArray::try_new(chunks, input_dtype.clone())?.into_array();
    let nested = ChunkedArray::try_new(vec![chunked], input_dtype)?.into_array();
    assert_eq!(sum_v2(&nested, &mut ctx)?, expected);
    Ok(())
}

#[rstest]
#[case::i64(8, DecimalValue::I64(99_999_999), 100_000_000_000)]
#[case::i128(28, DecimalValue::I128(10i128.pow(28) - 1), 100_000_000_000)]
#[case::i256(76, DecimalValue::I256(i256::from_i128(10).wrapping_pow(76) - i256::ONE), 10)]
fn constant_native_overflow_cancels(
    #[case] precision: u8,
    #[case] value: DecimalValue,
    #[case] len: usize,
) -> VortexResult<()> {
    let dtype = DecimalDType::new(precision, 0);
    let scalar = Scalar::decimal(value, dtype, Nullability::NonNullable);
    let negative = Scalar::decimal(
        value
            .checked_mul(&DecimalValue::I8(-1))
            .ok_or_else(|| vortex_err!("test value can be negated"))?,
        dtype,
        Nullability::NonNullable,
    );
    let mut accumulator = Accumulator::try_new(
        SumV2,
        NumericalAggregateOpts::default(),
        scalar.dtype().clone(),
    )?;
    let mut ctx = array_session().create_execution_ctx();
    accumulator.accumulate(&ConstantArray::new(scalar, len).into_array(), &mut ctx)?;
    let partial = accumulator.flush()?;
    accumulator.accumulate(&ConstantArray::new(negative, len).into_array(), &mut ctx)?;
    accumulator.combine_partials(partial)?;
    let expected_dtype = DecimalDType::new((precision + 10).min(76), 0);
    assert_eq!(
        accumulator.finish()?,
        Scalar::decimal(DecimalValue::I8(0), expected_dtype, Nullability::Nullable)
    );
    Ok(())
}

#[test]
fn grouped_final_precision_and_empty() -> VortexResult<()> {
    let dtype = DecimalDType::new(76, 0);
    let value = i256::from_i128(10).wrapping_pow(75) * i256::from_i128(6);
    let elements =
        DecimalArray::new(buffer![value, value, -value], dtype, Validity::NonNullable).into_array();
    let groups = ListViewArray::new(
        elements.clone(),
        buffer![0u32, 0, 0].into_array(),
        buffer![3u32, 2, 0].into_array(),
        Validity::NonNullable,
    );
    let mut accumulator = GroupedAccumulator::try_new(
        SumV2,
        NumericalAggregateOpts::default(),
        elements.dtype().clone(),
    )?;
    let mut ctx = array_session().create_execution_ctx();
    accumulator.accumulate_list(&groups.into_array(), &mut ctx)?;
    let expected = DecimalArray::new(
        buffer![value, i256::ZERO, i256::ZERO],
        dtype,
        Validity::from_iter([true, false, false]),
    )
    .into_array();
    assert_arrays_eq!(accumulator.finish()?, expected, &mut ctx);
    Ok(())
}
