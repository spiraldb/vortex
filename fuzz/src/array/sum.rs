// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use num_traits::CheckedAdd;
use num_traits::CheckedSub;
use num_traits::ToPrimitive;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::aggregate_fn::AggregateFnVTable;
use vortex_array::aggregate_fn::NumericalAggregateOpts;
use vortex_array::aggregate_fn::fns::sum::Sum;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::dtype::i256;
use vortex_array::match_each_decimal_value_type;
use vortex_array::match_each_native_ptype;
use vortex_array::scalar::DecimalValue;
use vortex_array::scalar::Scalar;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;

/// Compute the expected result of `sum` from the canonical form of the array.
///
/// `Sum` saturates to null as soon as any partial sum leaves the range of the return type, and
/// different encodings accumulate in different orders: a chunked array sums each chunk before
/// combining, a sparse array multiplies out its fill value, and so on. When some partial sums fit
/// and others do not, the result depends on how the array happens to be partitioned, so no single
/// expected value is correct. This returns `None` in that case so the caller can skip the check.
///
/// The result is well-defined in exactly two cases, decided from the exact sum `S` and the sum of
/// absolute values `M`, both computed in arithmetic wide enough not to overflow:
///
/// - `M` fits the return type: every partial sum is bounded by `M`, so no path can saturate and
///   the result is `S`.
/// - `S` does not fit the return type: the final checked add or precision check must fail on
///   every path, so the result is null.
pub fn sum_canonical_array(
    canonical: &Canonical,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<Scalar>> {
    let return_dtype = Sum
        .return_dtype(&NumericalAggregateOpts::default(), canonical.dtype())
        .ok_or_else(|| vortex_err!("Unsupported sum dtype: {}", canonical.dtype()))?;

    let Some(exact) = (match canonical {
        Canonical::Bool(array) => exact_sum_bool(array, ctx)?,
        Canonical::Primitive(array) => exact_sum_primitive(array, ctx)?,
        Canonical::Decimal(array) => exact_sum_decimal(array, ctx)?,
        _ => vortex_bail!("Unsupported sum dtype: {}", canonical.dtype()),
    }) else {
        // The wide arithmetic itself overflowed, so we cannot classify the result.
        return Ok(None);
    };

    // `sum` is only `Some` when the exact sum fits the return type.
    let (sum, magnitude_fits) = match (&return_dtype, exact) {
        (DType::Primitive(PType::U64, _), ExactSum::Unsigned(sum)) => {
            let sum = u64::try_from(sum)
                .ok()
                .map(|v| Scalar::primitive(v, Nullability::Nullable));
            let magnitude_fits = sum.is_some();
            (sum, magnitude_fits)
        }
        (DType::Primitive(PType::I64, _), ExactSum::Signed { sum, magnitude }) => (
            i64::try_from(sum)
                .ok()
                .map(|v| Scalar::primitive(v, Nullability::Nullable)),
            magnitude <= i64::MAX.unsigned_abs().into(),
        ),
        (DType::Decimal(decimal_dtype, _), ExactSum::Decimal { sum, magnitude }) => (
            fits_in_precision(sum, *decimal_dtype).then(|| {
                Scalar::decimal(
                    DecimalValue::from(sum),
                    *decimal_dtype,
                    Nullability::Nullable,
                )
            }),
            fits_in_precision(magnitude, *decimal_dtype),
        ),
        (dtype, _) => vortex_bail!("Unexpected sum return dtype: {dtype}"),
    };

    Ok(match sum {
        Some(sum) if magnitude_fits => Some(sum),
        None => Some(Scalar::null(return_dtype.as_nullable())),
        Some(_) => None,
    })
}

/// The exact sum of the valid values of an array alongside the sum of their absolute values.
enum ExactSum {
    /// Unsigned values, so the sum is its own magnitude.
    Unsigned(u128),
    Signed {
        sum: i128,
        magnitude: u128,
    },
    Decimal {
        sum: i256,
        magnitude: i256,
    },
}

fn exact_sum_bool(array: &BoolArray, ctx: &mut ExecutionCtx) -> VortexResult<Option<ExactSum>> {
    let mask = array.validity()?.execute_mask(array.len(), ctx)?;
    let count = array
        .to_bit_buffer()
        .iter()
        .zip(mask.iter())
        .filter(|(value, valid)| *value && *valid)
        .count();
    Ok(Some(ExactSum::Unsigned(count as u128)))
}

fn exact_sum_primitive(
    array: &PrimitiveArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ExactSum>> {
    let mask = array.validity()?.execute_mask(array.len(), ctx)?;
    match_each_native_ptype!(array.ptype(),
        integral: |T| {
            let mut values = array
                .as_slice::<T>()
                .iter()
                .zip(mask.iter())
                .filter(|(_, valid)| *valid)
                .map(|(v, _)| v.to_i128().vortex_expect("integral value fits in i128"));
            Ok(if array.ptype().is_unsigned_int() {
                values
                    .map(|v| v.unsigned_abs())
                    .try_fold(0u128, |acc, v| acc.checked_add(v))
                    .map(ExactSum::Unsigned)
            } else {
                values
                    .try_fold((0i128, 0u128), |(sum, magnitude), v| {
                        Some((sum.checked_add(v)?, magnitude.checked_add(v.unsigned_abs())?))
                    })
                    .map(|(sum, magnitude)| ExactSum::Signed { sum, magnitude })
            })
        },
        floating: |_T| {
            vortex_bail!("Float sums are not fuzzed: {}", array.ptype())
        }
    )
}

fn exact_sum_decimal(
    array: &DecimalArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ExactSum>> {
    let mask = array.validity()?.execute_mask(array.len(), ctx)?;
    match_each_decimal_value_type!(array.values_type(), |T| {
        Ok(array
            .buffer::<T>()
            .iter()
            .zip(mask.iter())
            .filter(|(_, valid)| *valid)
            .map(|(v, _)| DecimalValue::from(*v).as_i256())
            .try_fold((i256::ZERO, i256::ZERO), |(sum, magnitude), v| {
                Some((sum.checked_add(&v)?, magnitude.checked_add(&abs_i256(v)?)?))
            })
            .map(|(sum, magnitude)| ExactSum::Decimal { sum, magnitude }))
    })
}

fn abs_i256(value: i256) -> Option<i256> {
    if value < i256::ZERO {
        i256::ZERO.checked_sub(&value)
    } else {
        Some(value)
    }
}

fn fits_in_precision(value: i256, decimal_dtype: DecimalDType) -> bool {
    DecimalValue::from(value).fits_in_precision(decimal_dtype)
}

#[cfg(test)]
mod tests {
    use num_traits::CheckedMul;
    use rstest::rstest;
    use vortex_array::ArrayRef;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::aggregate_fn::fns::sum::sum;
    use vortex_array::arrays::ChunkedArray;
    use vortex_array::arrays::DecimalArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::dtype::DecimalDType;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::i256;
    use vortex_array::scalar::DecimalValue;
    use vortex_array::scalar::Scalar;
    use vortex_buffer::Buffer;
    use vortex_error::VortexResult;

    use super::*;
    use crate::SESSION;

    fn decimal_76() -> DecimalDType {
        DecimalDType::new(76, -76)
    }

    /// `n * 10^75`, close to the edge of precision 76.
    fn e75(n: i128) -> i256 {
        i256::from_i128(10)
            .checked_pow(75)
            .and_then(|v| v.checked_mul(&i256::from_i128(n)))
            .vortex_expect("fits in i256")
    }

    fn decimal_array(values: &[i256]) -> DecimalArray {
        DecimalArray::new(
            Buffer::from_iter(values.iter().copied()),
            decimal_76(),
            vortex_array::validity::Validity::NonNullable,
        )
    }

    fn decimal_scalar(value: i256) -> Scalar {
        Scalar::decimal(
            DecimalValue::from(value),
            decimal_76(),
            Nullability::Nullable,
        )
    }

    fn expected_sum(array: ArrayRef) -> VortexResult<Option<Scalar>> {
        let mut ctx = SESSION.create_execution_ctx();
        let canonical = array.execute::<Canonical>(&mut ctx)?;
        sum_canonical_array(&canonical, &mut ctx)
    }

    #[rstest]
    #[case::fits(&[e75(1), e75(2), e75(-1)], Some(decimal_scalar(e75(2))))]
    #[case::overflows(&[e75(6), e75(6)], Some(Scalar::null(DType::Decimal(decimal_76(), Nullability::Nullable))))]
    #[case::partial_overflows(&[e75(6), e75(6), e75(-6)], None)]
    #[case::cancels_to_zero(&[e75(6), e75(-6)], None)]
    fn decimal_sum_oracle(
        #[case] values: &[i256],
        #[case] expected: Option<Scalar>,
    ) -> VortexResult<()> {
        assert_eq!(expected_sum(decimal_array(values).into_array())?, expected);
        Ok(())
    }

    #[rstest]
    #[case::fits(&[1, 2, -1], Some(Scalar::primitive(2i64, Nullability::Nullable)))]
    #[case::overflows(&[i64::MAX, 1], Some(Scalar::null(DType::Primitive(PType::I64, Nullability::Nullable))))]
    #[case::partial_overflows(&[i64::MAX, 1, -1], None)]
    #[case::min_and_max(&[i64::MIN, i64::MAX], None)]
    fn signed_sum_oracle(
        #[case] values: &[i64],
        #[case] expected: Option<Scalar>,
    ) -> VortexResult<()> {
        let array = PrimitiveArray::from_iter(values.iter().copied()).into_array();
        assert_eq!(expected_sum(array)?, expected);
        Ok(())
    }

    #[rstest]
    #[case::fits(&[1, 2, 3], Some(Scalar::primitive(6u64, Nullability::Nullable)))]
    #[case::overflows(&[u64::MAX, 1], Some(Scalar::null(DType::Primitive(PType::U64, Nullability::Nullable))))]
    fn unsigned_sum_oracle(
        #[case] values: &[u64],
        #[case] expected: Option<Scalar>,
    ) -> VortexResult<()> {
        let array = PrimitiveArray::from_iter(values.iter().copied()).into_array();
        assert_eq!(expected_sum(array)?, expected);
        Ok(())
    }

    #[test]
    fn nulls_are_skipped() -> VortexResult<()> {
        let array = PrimitiveArray::from_option_iter([Some(1i64), None, Some(2)]).into_array();
        assert_eq!(
            expected_sum(array)?,
            Some(Scalar::primitive(3i64, Nullability::Nullable))
        );
        Ok(())
    }

    #[test]
    fn bool_sum_counts_valid_trues() -> VortexResult<()> {
        let array = BoolArray::from_iter([Some(true), Some(false), None, Some(true)]).into_array();
        assert_eq!(
            expected_sum(array)?,
            Some(Scalar::primitive(2u64, Nullability::Nullable))
        );
        Ok(())
    }

    /// The chunked layout from issue 9407: the first chunk's partial sum is out of precision even
    /// though the total fits, so `sum` saturates where the canonical array does not. The oracle
    /// must decline to predict this rather than pick a side.
    #[test]
    fn chunked_partial_overflow_is_skipped() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let chunked = ChunkedArray::try_new(
            [
                decimal_array(&[e75(6), e75(6)]).into_array(),
                decimal_array(&[e75(-6)]).into_array(),
            ],
            DType::Decimal(decimal_76(), Nullability::NonNullable),
        )?
        .into_array();

        assert!(sum(&chunked, &mut ctx)?.is_null());
        assert_eq!(
            sum(
                &decimal_array(&[e75(6), e75(6), e75(-6)]).into_array(),
                &mut ctx
            )?,
            decimal_scalar(e75(6))
        );
        assert_eq!(expected_sum(chunked)?, None);
        Ok(())
    }

    #[test]
    fn chunked_in_range_matches_oracle() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let chunked = ChunkedArray::try_new(
            [
                decimal_array(&[e75(1), e75(2)]).into_array(),
                decimal_array(&[e75(-1)]).into_array(),
            ],
            DType::Decimal(decimal_76(), Nullability::NonNullable),
        )?
        .into_array();

        let expected = expected_sum(chunked.clone())?;
        assert_eq!(expected, Some(decimal_scalar(e75(2))));
        assert_eq!(Some(sum(&chunked, &mut ctx)?), expected);
        Ok(())
    }
}
