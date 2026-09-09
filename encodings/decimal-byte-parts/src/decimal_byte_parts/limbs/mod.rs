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

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::DecimalType;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::dtype::i256;
use vortex_array::match_each_signed_integer_ptype;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_mask::Mask;

/// The maximum number of lower parts an encoded decimal can carry. Each is 64 bits.
///
/// Since the MSP is at most 64 bits wide, three additional 64-bit parts saturates
/// the 256-bit maximum width of a Vortex decimal.
pub const MAX_LOWER_PARTS: usize = 3;

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
    /// Construct decimal parts from an MSP with no lower parts.
    fn from_msp<T: NativePType>(values: Buffer<T>, validity: Validity) -> Self {
        Self {
            msp: PrimitiveArray::new(values, validity).into_array(),
            lower_parts: Vec::new(),
        }
    }

    fn new(
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
///
/// # Errors
///
/// Returns an error if the array's validity cannot be derived or executed.
pub fn split_decimal(decimal: &DecimalArray, ctx: &mut ExecutionCtx) -> VortexResult<DecimalParts> {
    let validity = decimal.validity()?;
    Ok(match decimal.values_type() {
        DecimalType::I8 => DecimalParts::from_msp(decimal.buffer::<i8>(), validity),
        DecimalType::I16 => DecimalParts::from_msp(decimal.buffer::<i16>(), validity),
        DecimalType::I32 => DecimalParts::from_msp(decimal.buffer::<i32>(), validity),
        DecimalType::I64 => DecimalParts::from_msp(decimal.buffer::<i64>(), validity),
        DecimalType::I128 => {
            let mask = validity.execute_mask(decimal.len(), ctx)?;
            let (msp, lower) = split_i128(&decimal.buffer::<i128>(), &mask);
            DecimalParts::new(msp, [lower], validity)
        }
        DecimalType::I256 => {
            let mask = validity.execute_mask(decimal.len(), ctx)?;
            let (msp, lower) = split_i256(&decimal.buffer::<i256>(), &mask);
            DecimalParts::new(msp, lower, validity)
        }
    })
}

/// Split each `i128` into an `i64` MSP and an `u64` lower part.
///
/// For each valid row, the original value is `msp * 2^64 + lower`. Invalid rows are zeroed.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "splitting a wide integer into 64-bit windows truncates by construction"
)]
fn split_i128(values: &Buffer<i128>, validity: &Mask) -> (Buffer<i64>, Buffer<u64>) {
    if validity.all_true() {
        let mut msp = BufferMut::<i64>::with_capacity(values.len());
        let mut lower = BufferMut::<u64>::with_capacity(values.len());
        for value in values.iter() {
            msp.push((value >> LOWER_PART_BITS) as i64);
            lower.push(*value as u64);
        }
        return (msp.freeze(), lower.freeze());
    }

    // Lower parts are stored as non-nullable arrays, so use zeros at null positions instead
    // of copying their garbage values.
    let mut msp = BufferMut::<i64>::zeroed(values.len());
    let mut lower = BufferMut::<u64>::zeroed(values.len());

    if let Mask::Values(valid) = validity {
        let msp = msp.as_mut_slice();
        let lower = lower.as_mut_slice();
        valid.bit_buffer().for_each_set_index(|i| {
            let value = values[i];
            msp[i] = (value >> LOWER_PART_BITS) as i64;
            lower[i] = value as u64;
        });
    }
    (msp.freeze(), lower.freeze())
}

/// Split each `i256` into an `i64` MSP and three `u64` lower parts, ordered most significant
/// first.
///
/// For each valid row, the original value is
///
/// `msp * 2^192 + lower[0] * 2^128 + lower[1] * 2^64 + lower[2]`.
///
/// Invalid rows are zeroed.
fn split_i256(
    values: &Buffer<i256>,
    validity: &Mask,
) -> (Buffer<i64>, [Buffer<u64>; MAX_LOWER_PARTS]) {
    // With no nulls, append every value without zeroing the output buffers first.
    if validity.all_true() {
        let mut msp = BufferMut::<i64>::with_capacity(values.len());
        let mut lower = std::array::from_fn::<_, MAX_LOWER_PARTS, _>(|_| {
            BufferMut::<u64>::with_capacity(values.len())
        });
        for value in values.iter() {
            let [msp_word, lower_words @ ..] = i256_to_words(*value);
            msp.push(msp_word.cast_signed());
            for (part, word) in lower.iter_mut().zip(lower_words) {
                part.push(word);
            }
        }
        return (msp.freeze(), lower.map(BufferMut::freeze));
    }

    // Lower parts are stored as non-nullable arrays, so use zeros at null positions instead
    // of copying their garbage values.
    let mut msp = BufferMut::<i64>::zeroed(values.len());
    let mut lower =
        std::array::from_fn::<_, MAX_LOWER_PARTS, _>(|_| BufferMut::<u64>::zeroed(values.len()));

    if let Mask::Values(valid) = validity {
        let msp = msp.as_mut_slice();
        let mut lower = lower.each_mut().map(BufferMut::as_mut_slice);
        valid.bit_buffer().for_each_set_index(|i| {
            let [msp_word, lower_words @ ..] = i256_to_words(values[i]);
            msp[i] = msp_word.cast_signed();
            for (part, word) in lower.iter_mut().zip(lower_words) {
                part[i] = word;
            }
        });
    }
    (msp.freeze(), lower.map(BufferMut::freeze))
}

/// Split an `i256` into four `u64` words, most significant first.
#[inline]
const fn i256_to_words(value: i256) -> [u64; 4] {
    let (low, high) = value.to_parts();
    #[expect(
        clippy::cast_possible_truncation,
        reason = "each cast takes the low 64 bits of a word pair by construction"
    )]
    [
        (high >> LOWER_PART_BITS) as u64,
        high as u64,
        (low >> LOWER_PART_BITS) as u64,
        low as u64,
    ]
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
pub(crate) fn assemble_decimal(
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
        .map(|part| {
            vortex_ensure!(
                part.dtype() == &LOWER_PART_DTYPE,
                "lower part must be non-nullable u64"
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
        [first] => DecimalArray::new(assemble_i128(msp, first), decimal_dtype, validity),
        [first, second] => {
            DecimalArray::new(assemble_i256(msp, [first, second]), decimal_dtype, validity)
        }
        [first, second, third] => DecimalArray::new(
            assemble_i256(msp, [first, second, third]),
            decimal_dtype,
            validity,
        ),
        _ => vortex_bail!(
            "at most {MAX_LOWER_PARTS} lower parts are supported, got {}",
            lower.len()
        ),
    })
}

/// Reassemble a signed MSP and one `u64` lower part into `i128` values.
///
/// For each row, the result is `msp * 2^64 + lower`.
#[expect(
    clippy::useless_conversion,
    reason = "the widening to i64 is a no-op only for the i64 arm of the ptype match"
)]
fn assemble_i128(msp: &PrimitiveArray, lower: &[u64]) -> Buffer<i128> {
    let mut out = BufferMut::<i128>::zeroed(msp.len());
    match_each_signed_integer_ptype!(msp.ptype(), |P| {
        for ((slot, value), part) in out
            .as_mut_slice()
            .iter_mut()
            .zip(msp.as_slice::<P>())
            .zip(lower)
        {
            // Sign-extend the MSP, then shift it into the high 64 bits. The unsigned
            // lower part fills the low 64 bits.
            *slot = (i128::from(i64::from(*value)) << LOWER_PART_BITS) | i128::from(*part);
        }
    });
    out.freeze()
}

/// Reassemble a signed MSP and two or three `u64` lower parts into `i256` values.
///
/// The last two lower parts form the unsigned low 128 bits. With two lower parts, the
/// signed high 128 bits are the MSP widened to `i128`. With three, the high half contains
/// the MSP followed by the first lower part.
#[expect(
    clippy::useless_conversion,
    reason = "the widening to i64 is a no-op only for the i64 arm of the ptype match"
)]
fn assemble_i256<const K: usize>(msp: &PrimitiveArray, lower: [&[u64]; K]) -> Buffer<i256> {
    let mut out = BufferMut::<i256>::with_capacity(msp.len());
    match_each_signed_integer_ptype!(msp.ptype(), |P| {
        for (row, value) in msp.as_slice::<P>().iter().enumerate() {
            // The last two lower parts always form the unsigned low 128 bits.
            let low =
                (u128::from(lower[K - 2][row]) << LOWER_PART_BITS) | u128::from(lower[K - 1][row]);
            let msp = i128::from(i64::from(*value));
            let high = if K == 2 {
                // Widening the MSP supplies the remaining sign bits.
                msp
            } else {
                // With three lower parts, the first one follows the MSP in the high half.
                (msp << LOWER_PART_BITS) | i128::from(lower[0][row])
            };
            out.push(i256::from_parts(low, high));
        }
    });
    out.freeze()
}

#[cfg(test)]
mod tests;
