// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Splitting decimal values into 64-bit parts and reassembling them.
//!
//! A `DecimalByteParts` array stores each value as a signed most significant part (MSP)
//! followed by `k` unsigned 64-bit lower parts ordered most significant first. The encoded
//! value is
//!
//! ```text
//! msp * 2^(64k) + Σ_{i<k} lower[i] * 2^(64 * (k - 1 - i))
//! ```
//!
//! This is exactly the two's complement bit pattern of the decimal value cut on 64-bit
//! boundaries.

use std::ops::BitOr;
use std::ops::Shl;

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::DecimalType;
use vortex_array::dtype::NativeDecimalType;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::dtype::i256;
use vortex_array::match_each_signed_integer_ptype;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_mask::Mask;

/// The maximum number of 64-bit lower parts an encoded `i128` decimal can carry.
pub const MAX_I128_LOWER_PARTS: usize = 1;

/// The maximum number of 64-bit lower parts an encoded `i256` decimal can carry.
pub const MAX_I256_LOWER_PARTS: usize = 3;

/// The maximum number of 64-bit lower parts an encoded decimal can carry.
///
/// Since the MSP is at most 64 bits wide, three additional 64-bit parts saturates
/// the 256-bit maximum width of a Vortex decimal.
pub const MAX_LOWER_PARTS: usize = MAX_I256_LOWER_PARTS;

/// Number of bits stored in each lower part.
const LOWER_PART_BITS: usize = 64;

/// Every lower part is a non-nullable `u64` primitive, since the MSP carries the sign
/// and validity.
pub(crate) const LOWER_PART_DTYPE: DType = DType::Primitive(PType::U64, Nullability::NonNullable);

/// A decimal array decomposed into byte parts.
pub struct DecimalParts {
    /// The signed most significant part. This carries the validity of the whole array.
    pub msp: ArrayRef,
    /// The unsigned 64-bit lower parts, most significant first.
    pub lower_parts: Vec<ArrayRef>,
}

impl DecimalParts {
    /// Construct decimal parts from the MSP buffer constituting a narrow decimal (`i64` or narrower).
    /// Narrow decimals have an MSP at most as wide as `i64` and no lower parts.
    fn from_narrow<T: NativePType>(values: Buffer<T>, validity: Validity) -> Self {
        Self {
            msp: PrimitiveArray::new(values, validity).into_array(),
            lower_parts: Vec::new(),
        }
    }

    /// Construct decimal parts arrays from the buffers constituting a wide decimal (`i128` or `i256`).
    /// Wide decimals have an `i64` MSP and up to [`MAX_LOWER_PARTS`] `u64` lower parts.
    fn from_wide(
        msp: Buffer<i64>,
        lower_parts: impl IntoIterator<Item = Buffer<u64>>,
        validity: Validity,
    ) -> Self {
        Self {
            msp: PrimitiveArray::new(msp, validity).into_array(),
            lower_parts: lower_parts
                .into_iter()
                .map(|part| PrimitiveArray::new(part, Validity::NonNullable).into_array())
                .collect(),
        }
    }
}

/// Split a canonical decimal array into a signed most significant part (MSP) and unsigned 64-bit
/// lower parts. The MSP is at most 64 bits.
///
/// Values narrower than 128 bits are already a single signed part, so they are returned
/// with no lower parts. `i128` values split into an `i64` MSP and one lower part. `i256`
/// values split into an `i64` MSP and three lower parts.
///
/// The MSP retains the decimal's validity while lower parts are non-nullable. Lower parts
/// are constructed with zeroes at null positions instead of invalid bytes.
/// Empty and all-null arrays use constant parts, preserving the part types and MSP's nullability.
///
/// # Errors
///
/// * If the array's validity cannot be derived or executed.
pub fn split_decimal(decimal: &DecimalArray, ctx: &mut ExecutionCtx) -> VortexResult<DecimalParts> {
    let validity = decimal.validity()?;
    let len = decimal.len();
    let mask = validity.execute_mask(len, ctx)?;

    if mask.all_false() || decimal.is_empty() {
        return Ok(split_no_valid_row(decimal, &validity));
    }

    Ok(match decimal.values_type() {
        DecimalType::I8 => DecimalParts::from_narrow(decimal.buffer::<i8>(), validity),
        DecimalType::I16 => DecimalParts::from_narrow(decimal.buffer::<i16>(), validity),
        DecimalType::I32 => DecimalParts::from_narrow(decimal.buffer::<i32>(), validity),
        DecimalType::I64 => DecimalParts::from_narrow(decimal.buffer::<i64>(), validity),
        DecimalType::I128 => {
            let (msp, lower) = split_wide(&decimal.buffer::<i128>(), &mask, i128_to_parts);
            DecimalParts::from_wide(msp, lower, validity)
        }
        DecimalType::I256 => {
            let (msp, lower) = split_wide(&decimal.buffer::<i256>(), &mask, i256_to_parts);
            DecimalParts::from_wide(msp, lower, validity)
        }
    })
}

/// Splits decimals with no valid rows (all null or empty) into constant decimal parts with the
/// corresponding nullability.
fn split_no_valid_row(decimal: &DecimalArray, validity: &Validity) -> DecimalParts {
    let (msp_ptype, lower_part_count) = match decimal.values_type() {
        DecimalType::I8 => (PType::I8, 0),
        DecimalType::I16 => (PType::I16, 0),
        DecimalType::I32 => (PType::I32, 0),
        DecimalType::I64 => (PType::I64, 0),
        DecimalType::I128 => (PType::I64, MAX_I128_LOWER_PARTS),
        DecimalType::I256 => (PType::I64, MAX_I256_LOWER_PARTS),
    };
    // Empty masks are also all-false. The default scalar is null for nullable inputs
    // and zero for non-nullable empty inputs, preserving the MSP's nullability.
    let msp = Scalar::default_value(&DType::Primitive(msp_ptype, validity.nullability()));
    let len = decimal.len();
    DecimalParts {
        msp: ConstantArray::new(msp, len).into_array(),
        lower_parts: vec![ConstantArray::new(0u64, len).into_array(); lower_part_count],
    }
}

/// Split wide integers into a signed MSP and `N` unsigned lower parts.
///
/// `to_parts` returns the MSP and lower words in most-significant-first order.
/// It is specialized for each input type: `i128` has one lower word and `i256`
/// has three. Null rows get zeros in every output buffer. The caller handles empty
/// and all-null arrays before calling this function.
fn split_wide<T: Copy, const N: usize>(
    values: &Buffer<T>,
    validity: &Mask,
    to_parts: impl Fn(T) -> (i64, [u64; N]),
) -> (Buffer<i64>, [Buffer<u64>; N]) {
    let len = values.len();
    let mut msp = BufferMut::<i64>::with_capacity(len);
    let mut lower = std::array::from_fn::<_, N, _>(|_| BufferMut::<u64>::with_capacity(len));

    // Allocate without zeroing, then initialize every part of each row together.
    let msp_out = &mut msp.spare_capacity_mut()[..len];
    let mut lower_out = lower
        .each_mut()
        .map(|part| &mut part.spare_capacity_mut()[..len]);

    match validity {
        Mask::AllTrue(_) => {
            for row in 0..len {
                let (high, words) = to_parts(values[row]);
                msp_out[row].write(high);
                for (part, word) in lower_out.iter_mut().zip(words) {
                    part[row].write(word);
                }
            }
        }
        Mask::Values(validity) => {
            // A shorter bitmap would leave output slots uninitialized before set_len.
            assert_eq!(
                validity.bit_buffer().len(),
                len,
                "values and validity must have the same length"
            );
            for (chunk_index, ((chunk, bits), msp)) in values
                .chunks(64)
                .zip(validity.bit_buffer().chunks().iter_padded())
                .zip(msp_out.chunks_mut(64))
                .enumerate()
            {
                for (i, (&value, msp)) in chunk.iter().zip(msp).enumerate() {
                    let mask = 0u64.wrapping_sub((bits >> i) & 1);
                    let (high, words) = to_parts(value);
                    msp.write(high & mask.cast_signed());
                    for (part, word) in lower_out.iter_mut().zip(words) {
                        part[chunk_index * 64 + i].write(word & mask);
                    }
                }
            }
        }
        Mask::AllFalse(_) => unreachable!("all-null arrays are handled by split_decimal"),
    }

    // SAFETY: the input and all output slices have len elements. Both branches
    // initialize every slot, including null rows and the final partial chunk.
    // The bitmap length check prevents the masked iteration from ending early.
    unsafe {
        msp.set_len(len);
        for part in &mut lower {
            part.set_len(len);
        }
    }
    (msp.freeze(), lower.map(BufferMut::freeze))
}

/// Extract the high signed word and low unsigned word of an `i128`.
#[inline]
const fn i128_to_parts(value: i128) -> (i64, [u64; MAX_I128_LOWER_PARTS]) {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "each cast preserves a 64-bit window of the original two's complement bits"
    )]
    ((value >> LOWER_PART_BITS) as i64, [value as u64])
}

/// Extract the signed MSP and three unsigned lower words of an `i256`.
#[inline]
const fn i256_to_parts(value: i256) -> (i64, [u64; MAX_I256_LOWER_PARTS]) {
    let (low, high) = value.to_parts();
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "each cast preserves a 64-bit window of the original two's complement bits"
    )]
    (
        (high >> LOWER_PART_BITS) as i64,
        [high as u64, (low >> LOWER_PART_BITS) as u64, low as u64],
    )
}

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
    T: NativeDecimalType + Shl<usize, Output = T> + BitOr<Output = T>,
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
    T: NativeDecimalType + Shl<usize, Output = T> + BitOr<Output = T>,
{
    let mut value = T::from(msp).vortex_expect("MSP fits in the output type");
    for part in lower {
        value = (value << LOWER_PART_BITS)
            | T::from(part).vortex_expect("lower word fits in the output type");
    }
    value
}

#[cfg(test)]
mod tests;
