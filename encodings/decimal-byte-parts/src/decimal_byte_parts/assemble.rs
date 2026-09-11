// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Reassembling decimal arrays and values from their parts.

use std::ops::BitOr;
use std::ops::Shl;

use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::NativeDecimalType;
use vortex_array::dtype::i256;
use vortex_array::match_each_signed_integer_ptype;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;

use super::LOWER_PART_BITS;
use super::LOWER_PART_DTYPE;
use super::MAX_LOWER_PARTS;

/// Reassemble primitive arrays that constitute decimal byte parts into a canonical decimal array.
///
/// The MSP must be signed. There must be between zero and three (inclusive) `u64` lower parts, ordered
/// most significant first. The lower parts must be non-nullable. Every input array must have the same length.
///
/// With no lower parts, the MSP buffer is reused as the decimal values. One lower part
/// assembles into `i128`. Two or three lower parts assemble into `i256`.
///
/// # Errors
///
/// Returns an error if the parts do not describe a valid decimal, or if the MSP's validity
/// cannot be derived.
pub fn assemble_decimal(
    msp: &PrimitiveArray,
    lower_parts: &[PrimitiveArray],
    decimal_dtype: DecimalDType,
) -> VortexResult<DecimalArray> {
    let validity = msp.validity()?;
    vortex_ensure!(msp.dtype().as_ptype().is_signed_int());

    if lower_parts.is_empty() {
        return Ok(match_each_signed_integer_ptype!(msp.ptype(), |P| {
            // SAFETY: the buffer is typed by the array's own ptype, the decimal dtype is the
            // array's, and the validity is taken from the same array.
            unsafe { DecimalArray::new_unchecked(msp.to_buffer::<P>(), decimal_dtype, validity) }
        }));
    }

    let len = msp.len();
    let lower: Vec<&[u64]> = lower_parts
        .iter()
        .enumerate()
        .map(|(idx, part)| {
            vortex_ensure!(
                part.dtype() == &LOWER_PART_DTYPE,
                "lower part {idx} must have dtype {LOWER_PART_DTYPE}, got {}",
                part.dtype()
            );
            let part = part.as_slice::<u64>();
            vortex_ensure!(
                part.len() == len,
                "lower part has len {}, expected {len}",
                part.len()
            );
            Ok(part)
        })
        .collect::<VortexResult<_>>()?;

    Ok(match lower.as_slice() {
        [first] => DecimalArray::new(
            assemble_wide_decimal::<i128, 1>(msp, [first]),
            decimal_dtype,
            validity,
        ),
        [first, second] => DecimalArray::new(
            assemble_wide_decimal::<i256, 2>(msp, [first, second]),
            decimal_dtype,
            validity,
        ),
        [first, second, third] => DecimalArray::new(
            assemble_wide_decimal::<i256, 3>(msp, [first, second, third]),
            decimal_dtype,
            validity,
        ),
        _ => vortex_bail!(
            "at most {MAX_LOWER_PARTS} lower parts are supported, got {}",
            lower.len()
        ),
    })
}

/// Assemble a column of wide decimal values from the MSP and `K` lower-part columns.
///
/// A fixed part count lets the compiler unroll each call to [`assemble_wide_decimal_value`].
fn assemble_wide_decimal<T, const K: usize>(msp: &PrimitiveArray, lower: [&[u64]; K]) -> Buffer<T>
where
    T: NativeDecimalType + From<i64> + From<u64> + Shl<usize, Output = T> + BitOr<Output = T>,
{
    let mut out = BufferMut::<T>::with_capacity(msp.len());
    match_each_signed_integer_ptype!(msp.ptype(), |P| {
        out.extend_trusted(msp.as_slice::<P>().iter().enumerate().map(|(row, value)| {
            #[allow(
                clippy::useless_conversion,
                reason = "the widening to i64 is a no-op only for the i64 arm of the ptype match"
            )]
            let msp = i64::from(*value);
            assemble_wide_decimal_value(msp, lower.map(|part| part[row]))
        }));
    });
    out.freeze()
}

/// Reassemble a decimal's unscaled integer from its signed MSP and `K` lower words.
///
/// Sign-extend the MSP to `T`, then append each lower word by shifting left 64 bits and
/// filling the low bits. Lower words are ordered most significant first. Callers select
/// `i128` for one lower word and `i256` for two or three.
#[inline]
pub(crate) fn assemble_wide_decimal_value<T, const K: usize>(msp: i64, lower: [u64; K]) -> T
where
    T: NativeDecimalType + From<i64> + From<u64> + Shl<usize, Output = T> + BitOr<Output = T>,
{
    let mut value: T = msp.into();
    for part in lower {
        value = (value << LOWER_PART_BITS) | part.into();
    }
    value
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::array_session;
    use vortex_array::arrays::BoolArray;
    use vortex_array::arrays::Constant;
    use vortex_array::arrays::DecimalArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::DecimalDType;
    use vortex_array::dtype::DecimalType;
    use vortex_array::dtype::NativeDecimalType;
    use vortex_array::dtype::PType;
    use vortex_array::dtype::i256;
    use vortex_array::match_each_decimal_value_type;
    use vortex_array::validity::Validity;
    use vortex_buffer::Buffer;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;

    use super::assemble_decimal;
    use crate::decimal_byte_parts::split_decimal;

    #[rstest]
    #[case::empty_non_nullable(0, Validity::NonNullable)]
    #[case::empty_nullable(0, Validity::AllValid)]
    #[case::empty_all_null(0, Validity::AllInvalid)]
    #[case::all_null(3, Validity::AllInvalid)]
    #[case::all_null_array(3, Validity::Array(BoolArray::from_iter([false; 3]).into_array()))]
    fn test_split_without_valid_rows(
        #[case] len: usize,
        #[case] validity: Validity,
        #[values(
            DecimalType::I8,
            DecimalType::I16,
            DecimalType::I32,
            DecimalType::I64,
            DecimalType::I128,
            DecimalType::I256
        )]
        values_type: DecimalType,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let decimal = match_each_decimal_value_type!(values_type, |T| {
            DecimalArray::new(
                Buffer::<T>::zeroed(len),
                DecimalDType::new(T::MAX_PRECISION, 0),
                validity,
            )
        });
        let parts = split_decimal(&decimal, &mut ctx)?;
        assert!(parts.msp.is::<Constant>());
        assert!(parts.lower_parts.iter().all(|part| part.is::<Constant>()));
        assert_eq!(parts.msp.len(), len);
        assert_eq!(
            parts.msp.dtype().nullability(),
            decimal.dtype().nullability()
        );
        let round_tripped = round_trip(decimal.clone())?;
        assert_eq!(round_tripped.values_type(), values_type);
        assert_arrays_eq!(decimal, round_tripped, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[case::non_nullable(Validity::NonNullable)]
    #[case::all_valid(Validity::AllValid)]
    #[case::all_null(Validity::AllInvalid)]
    #[case::mixed(Validity::from_iter((0..263).map(|i| i % 3 != 1)))]
    #[case::sparse(Validity::from_iter((0..263).map(|i| i % 16 == 0)))]
    #[case::null_prefix_and_suffix(Validity::from_iter((0..263).map(|i| (67..196).contains(&i))))]
    fn test_split_zeroes_null_words(
        #[case] validity: Validity,
        #[values(false, true)] wide_256: bool,
        #[values(0, 1, 63, 64, 65, 257)] len: usize,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let decimal = if wide_256 {
            DecimalArray::new(
                buffer![i256::from_i128(-1); 263],
                DecimalDType::new(76, 2),
                validity,
            )
        } else {
            DecimalArray::new(buffer![-1i128; 263], DecimalDType::new(38, 2), validity)
        };
        let decimal = decimal
            .slice(3..len + 3)?
            .execute::<DecimalArray>(&mut ctx)?;
        let mask = decimal.validity()?.execute_mask(len, &mut ctx)?;
        let expected = PrimitiveArray::new(
            mask.iter()
                .map(|valid| if valid { u64::MAX } else { 0 })
                .collect::<Buffer<_>>(),
            Validity::NonNullable,
        );
        let parts = split_decimal(&decimal, &mut ctx)?;
        assert_eq!(parts.lower_parts.len(), if wide_256 { 3 } else { 1 });
        assert_eq!(
            parts.msp.dtype(),
            &DType::Primitive(PType::I64, decimal.dtype().nullability())
        );
        for lower in parts.lower_parts {
            assert_arrays_eq!(expected.clone(), lower, &mut ctx);
        }
        assert_arrays_eq!(decimal.clone(), round_trip(decimal)?, &mut ctx);
        Ok(())
    }

    fn round_trip(decimal: DecimalArray) -> VortexResult<DecimalArray> {
        let mut ctx = array_session().create_execution_ctx();
        let parts = split_decimal(&decimal, &mut ctx)?;
        let msp = parts.msp.execute::<PrimitiveArray>(&mut ctx)?;
        let lower = parts
            .lower_parts
            .into_iter()
            .map(|part| part.execute::<PrimitiveArray>(&mut ctx))
            .collect::<VortexResult<Vec<_>>>()?;
        assemble_decimal(&msp, &lower, decimal.decimal_dtype())
    }

    #[rstest]
    #[case::zero(0)]
    #[case::one(1)]
    #[case::minus_one(-1)]
    #[case::limb_boundary(1i128 << 64)]
    #[case::just_below_limb_boundary((1i128 << 64) - 1)]
    #[case::negative_limb_boundary(-(1i128 << 64))]
    #[case::max(i128::MAX)]
    #[case::min(i128::MIN)]
    fn test_split_assemble_i128(#[case] value: i128) -> VortexResult<()> {
        let decimal = DecimalArray::new(
            Buffer::from(vec![value]),
            DecimalDType::new(38, 2),
            Validity::NonNullable,
        );
        let round_tripped = round_trip(decimal)?;
        assert_eq!(round_tripped.buffer::<i128>().as_slice(), &[value]);
        Ok(())
    }

    #[rstest]
    #[case::zero(i256::ZERO)]
    #[case::one(i256::ONE)]
    #[case::minus_one(i256::ZERO - i256::ONE)]
    #[case::max(i256::MAX)]
    #[case::min(i256::MIN)]
    #[case::word_1(i256::from_parts(1u128 << 64, 0))]
    #[case::word_2(i256::from_parts(0, 1))]
    #[case::word_3(i256::from_parts(0, 1i128 << 64))]
    #[case::mixed(i256::from_parts(u128::MAX, -3))]
    fn test_split_assemble_i256(#[case] value: i256) -> VortexResult<()> {
        let decimal = DecimalArray::new(
            Buffer::from(vec![value]),
            DecimalDType::new(76, 2),
            Validity::NonNullable,
        );
        let round_tripped = round_trip(decimal)?;
        assert_eq!(round_tripped.buffer::<i256>().as_slice(), &[value]);
        Ok(())
    }

    #[rstest]
    fn test_split_narrow_decimal_reuses_values(
        #[values(Validity::NonNullable, Validity::from_iter([true, false, true]))]
        validity: Validity,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let decimal = DecimalArray::new(buffer![1i32, 2, 3], DecimalDType::new(2, 0), validity);
        let parts = split_decimal(&decimal, &mut ctx)?;
        assert!(parts.lower_parts.is_empty());
        assert_eq!(parts.msp.dtype().as_ptype(), PType::I32);
        let msp = parts.msp.execute::<PrimitiveArray>(&mut ctx)?;
        assert_eq!(
            msp.as_slice::<i32>().as_ptr(),
            decimal.buffer::<i32>().as_ptr()
        );
        assert_arrays_eq!(decimal.clone(), round_trip(decimal)?, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[case::signed(PrimitiveArray::new(buffer![0i64; 2], Validity::NonNullable))]
    #[case::narrow_unsigned(PrimitiveArray::new(buffer![0u32; 2], Validity::NonNullable))]
    #[case::nullable_all_valid(PrimitiveArray::new(buffer![0u64; 2], Validity::AllValid))]
    #[case::nullable_all_null(PrimitiveArray::new(buffer![0u64; 2], Validity::AllInvalid))]
    #[case::nullable_mixed(PrimitiveArray::new(buffer![0u64; 2], Validity::from_iter([true, false])))]
    fn test_assemble_rejects_invalid_lower_dtype(
        #[case] invalid_lower: PrimitiveArray,
        #[values(1, 2, 3)] lower_count: usize,
    ) {
        let msp = PrimitiveArray::new(buffer![0i64; 2], Validity::NonNullable);
        let mut lower =
            vec![PrimitiveArray::new(buffer![0u64; 2], Validity::NonNullable); lower_count];
        lower[lower_count - 1] = invalid_lower;
        let dtype = DecimalDType::new(if lower_count == 1 { 38 } else { 76 }, 0);
        assert!(assemble_decimal(&msp, &lower, dtype).is_err());
    }

    #[rstest]
    fn test_assemble_rejects_mismatched_lower_lengths(
        #[values(1, 2, 3)] lower_count: usize,
        #[values(0, 1, 3)] lower_len: usize,
    ) {
        let msp = PrimitiveArray::new(buffer![0i64; 2], Validity::NonNullable);
        let mut lower =
            vec![PrimitiveArray::new(buffer![0u64; 2], Validity::NonNullable); lower_count];
        lower[lower_count - 1] =
            PrimitiveArray::new(buffer![0u64; lower_len], Validity::NonNullable);
        let dtype = DecimalDType::new(if lower_count == 1 { 38 } else { 76 }, 0);
        assert!(assemble_decimal(&msp, &lower, dtype).is_err());
    }

    #[rstest]
    fn test_assemble_i256_part_order_and_sign_extension(
        #[values(false, true)] narrow_msp: bool,
        #[values(2, 3)] lower_count: usize,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let msp = if narrow_msp {
            PrimitiveArray::new(buffer![3i8, -3], Validity::NonNullable)
        } else {
            PrimitiveArray::new(buffer![3i64, -3], Validity::NonNullable)
        };
        let lower =
            [4u64, 1, 2].map(|word| PrimitiveArray::new(buffer![word; 2], Validity::NonNullable));
        let dtype = DecimalDType::new(76, 0);
        let actual = assemble_decimal(&msp, &lower[3 - lower_count..], dtype)?;
        let low = (1u128 << 64) | 2;
        let expected = if lower_count == 2 {
            buffer![i256::from_parts(low, 3), i256::from_parts(low, -3)]
        } else {
            buffer![
                i256::from_parts(low, (3i128 << 64) | 4),
                i256::from_parts(low, (-3i128 << 64) | 4),
            ]
        };
        assert_arrays_eq!(
            DecimalArray::new(expected, dtype, Validity::NonNullable),
            actual,
            &mut ctx
        );
        Ok(())
    }
}
