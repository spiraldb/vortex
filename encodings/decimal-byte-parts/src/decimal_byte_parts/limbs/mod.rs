// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Splitting decimal values into 64-bit parts, and reassembling them.
//!
//! A `DecimalByteParts` array stores each value as a signed most significant part (MSP)
//! followed by `k` unsigned 64-bit lower parts ordered most significant first. The encoded
//! value is
//!
//! ```text
//! msp * 2^(64k) + Σ_{i<k} lower[i] * 2^(64 * (k - 1 - i))
//! ```
//!
//! which is exactly the two's complement bit pattern of the decimal value cut on 64-bit
//! boundaries: the MSP holds the sign and the leading bits, every lower part holds a raw
//! 64-bit window of the magnitude.

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

/// The maximum number of lower parts an encoded decimal can carry.
///
/// The most significant part is at most 64 bits wide, so three additional 64-bit parts
/// saturate the 256-bit maximum width of a Vortex decimal.
pub const MAX_LOWER_PARTS: usize = 3;

/// Number of bits stored in each lower part.
const LOWER_PART_BITS: usize = 64;

/// The dtype every lower part must have: a non-nullable `u64`.
///
/// Validity is carried by the most significant part alone.
pub(crate) const LOWER_PART_DTYPE: DType = DType::Primitive(PType::U64, Nullability::NonNullable);

/// A decimal array decomposed into byte parts.
pub struct DecimalParts {
    /// The signed most significant part, carrying the validity of the whole array.
    pub msp: ArrayRef,
    /// The unsigned 64-bit lower parts, most significant first.
    pub lower_parts: Vec<ArrayRef>,
}

/// The decimal storage type that reassembling the given parts produces.
///
/// # Errors
///
/// Returns an error if `msp_ptype` is not a signed integer, or if there are more than
/// [`MAX_LOWER_PARTS`] lower parts.
pub(crate) fn assembled_values_type(
    msp_ptype: PType,
    lower_part_count: usize,
) -> VortexResult<DecimalType> {
    if lower_part_count > MAX_LOWER_PARTS {
        vortex_bail!("at most {MAX_LOWER_PARTS} lower parts are supported, got {lower_part_count}");
    }
    if lower_part_count == 0 {
        return DecimalType::try_from(msp_ptype);
    }
    let bits = msp_ptype.bit_width() + LOWER_PART_BITS * lower_part_count;
    Ok(if bits <= 128 {
        DecimalType::I128
    } else {
        DecimalType::I256
    })
}

/// Split a canonical decimal array into a signed most significant part and unsigned 64-bit
/// lower parts.
///
/// Values narrower than 128 bits are already a single signed part, so they are returned
/// with no lower parts. `i128` values split into an `i64` MSP and one lower part, `i256`
/// values into an `i64` MSP and three lower parts.
/// Lower parts are non-nullable, with zeroes at null positions so arbitrary null-slot bytes
/// do not affect their compression. The MSP retains the decimal's validity.
///
/// # Errors
///
/// Returns an error if the array's validity cannot be derived or executed.
pub fn split_decimal(decimal: &DecimalArray, ctx: &mut ExecutionCtx) -> VortexResult<DecimalParts> {
    let validity = decimal.validity()?;
    Ok(match decimal.values_type() {
        DecimalType::I8 => DecimalParts::flat(decimal.buffer::<i8>(), validity),
        DecimalType::I16 => DecimalParts::flat(decimal.buffer::<i16>(), validity),
        DecimalType::I32 => DecimalParts::flat(decimal.buffer::<i32>(), validity),
        DecimalType::I64 => DecimalParts::flat(decimal.buffer::<i64>(), validity),
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

/// Reassemble decimal byte parts into a canonical decimal array.
///
/// The parts must already be canonical primitive arrays: a signed MSP, and `u64` lower
/// parts ordered most significant first.
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
    if lower_parts.is_empty() {
        return Ok(match_each_signed_integer_ptype!(msp.ptype(), |P| {
            // SAFETY: the buffer is typed by the array's own ptype, the decimal dtype is the
            // array's, and the validity is taken from the same array.
            unsafe { DecimalArray::new_unchecked(msp.to_buffer::<P>(), decimal_dtype, validity) }
        }));
    }

    // Slice every part to the MSP's length up front: the assembly loops then index slices the
    // compiler knows are long enough, so the per-row bounds checks fall away.
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
                part.len() >= len,
                "lower part has len {}, expected at least {len}",
                part.len()
            );
            Ok(&part[..len])
        })
        .collect::<VortexResult<_>>()?;

    // The part count is dispatched to a constant so every 64-bit word lands at a compile-time
    // index. Leaving it dynamic costs 1.8x on the `i256` path — see `benches/decimal_assemble.rs`.
    let values = match assembled_values_type(msp.ptype(), lower.len())? {
        // A single lower part can never widen to an `i256`: the MSP is at most 64 bits, so
        // 64 + 64 fits an `i128` and takes the branch below.
        DecimalType::I256 => match lower.as_slice() {
            [first, second] => assemble_i256(msp, [first, second]),
            [first, second, third] => assemble_i256(msp, [first, second, third]),
            _ => vortex_bail!("unsupported lower part count {}", lower.len()),
        },
        _ => {
            return Ok(DecimalArray::new(
                assemble_i128(msp, lower[0]),
                decimal_dtype,
                validity,
            ));
        }
    };
    Ok(DecimalArray::new(values, decimal_dtype, validity))
}

/// 64-bit words in an `i256`.
const VALUE_WORDS: usize = 4;

/// The 64-bit words of an `i256`, ascending significance.
///
/// An `i256` is exactly `{_0: u64, _1: u64, _2: u64, _3: i64}` — three unsigned words beneath
/// a single signed one — which is the same shape this encoding stores. That is why splitting
/// and reassembling are pure reinterpretation rather than arithmetic: no carry ever crosses a
/// word boundary, so each word can be compressed independently and put back verbatim.
///
/// The sign lives in the most significant word alone. When the most significant part is
/// narrower than 64 bits, or sits below word 3, the words above it are its sign extension.
type ValueWords = [u64; VALUE_WORDS];

/// Reinterpret an `i256` as its 64-bit words.
#[inline]
const fn i256_to_words(value: i256) -> ValueWords {
    let (low, high) = value.to_parts();
    #[expect(
        clippy::cast_possible_truncation,
        reason = "each cast takes the low 64 bits of a word pair by construction"
    )]
    [
        low as u64,
        (low >> LOWER_PART_BITS) as u64,
        high as u64,
        (high >> LOWER_PART_BITS) as u64,
    ]
}

/// Reinterpret 64-bit words as an `i256`, with the most significant word carrying the sign.
#[inline]
const fn i256_from_words(words: ValueWords) -> i256 {
    i256::from_parts(
        (words[0] as u128) | ((words[1] as u128) << LOWER_PART_BITS),
        ((words[2] as u128) | ((words[3] as u128) << LOWER_PART_BITS)) as i128,
    )
}

/// The words of a value whose most significant part sits at `msp_word`, with every word above
/// it filled with the MSP's sign.
#[inline]
fn sign_extended_words(msp: i64, msp_word: usize) -> ValueWords {
    let mut words = [if msp < 0 { u64::MAX } else { 0 }; VALUE_WORDS];
    words[msp_word] = msp.cast_unsigned();
    words
}

impl DecimalParts {
    /// Parts for a decimal already stored in a single signed integer.
    fn flat<T: NativePType>(values: Buffer<T>, validity: Validity) -> Self {
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

/// The inverse of [`assemble_i256`] at `K == MAX_LOWER_PARTS`: word 3 becomes the signed MSP,
/// and words 2, 1, 0 become the lower parts, most significant first.
fn split_i256(
    values: &Buffer<i256>,
    validity: &Mask,
) -> (Buffer<i64>, [Buffer<u64>; MAX_LOWER_PARTS]) {
    if validity.all_true() {
        let mut msp = BufferMut::<i64>::with_capacity(values.len());
        let mut lower = std::array::from_fn::<_, MAX_LOWER_PARTS, _>(|_| {
            BufferMut::<u64>::with_capacity(values.len())
        });
        for value in values.iter() {
            let words = i256_to_words(*value);
            msp.push(words[MAX_LOWER_PARTS].cast_signed());
            for (part, word) in lower
                .iter_mut()
                .zip(words.iter().take(MAX_LOWER_PARTS).rev())
            {
                part.push(*word);
            }
        }
        return (msp.freeze(), lower.map(BufferMut::freeze));
    }

    let mut msp = BufferMut::<i64>::zeroed(values.len());
    let mut lower =
        std::array::from_fn::<_, MAX_LOWER_PARTS, _>(|_| BufferMut::<u64>::zeroed(values.len()));
    if let Mask::Values(valid) = validity {
        let msp = msp.as_mut_slice();
        let mut lower = lower.each_mut().map(BufferMut::as_mut_slice);
        valid.bit_buffer().for_each_set_index(|i| {
            let words = i256_to_words(values[i]);
            msp[i] = words[MAX_LOWER_PARTS].cast_signed();
            for (part, word) in lower
                .iter_mut()
                .zip(words.iter().take(MAX_LOWER_PARTS).rev())
            {
                part[i] = *word;
            }
        });
    }
    (msp.freeze(), lower.map(BufferMut::freeze))
}

/// Only one lower part can share 128 bits with a signed MSP, so this shape is fixed.
#[expect(
    clippy::useless_conversion,
    reason = "the widening to i64 is a no-op only for the i64 arm of the ptype match"
)]
fn assemble_i128(msp: &PrimitiveArray, lower: &[u64]) -> Buffer<i128> {
    // Store into a pre-sized buffer rather than pushing into a reserved one: at 16 bytes per
    // row the bounds-checked `push` dominates, and dropping it is 1.6x — see
    // `i128_row_write` against `i128_row_const` in `benches/decimal_assemble.rs`. The same
    // shape does not pay off for `i256`, where zeroing 32 bytes per row costs more than the
    // push it saves.
    let mut out = BufferMut::<i128>::zeroed(msp.len());
    match_each_signed_integer_ptype!(msp.ptype(), |P| {
        for ((slot, value), part) in out
            .as_mut_slice()
            .iter_mut()
            .zip(msp.as_slice::<P>())
            .zip(lower)
        {
            *slot = (i128::from(i64::from(*value)) << LOWER_PART_BITS) | i128::from(*part);
        }
    });
    out.freeze()
}

/// The lower parts fill the least significant 64-bit words, the MSP the word above them, and
/// the remaining high words are the MSP's sign extension.
///
/// `K` is a constant so the word indices are compile-time constants and the placement loop
/// unrolls; the same loop with a runtime part count is 1.8x slower.
#[expect(
    clippy::useless_conversion,
    reason = "the widening to i64 is a no-op only for the i64 arm of the ptype match"
)]
fn assemble_i256<const K: usize>(msp: &PrimitiveArray, lower: [&[u64]; K]) -> Buffer<i256> {
    let mut out = BufferMut::<i256>::with_capacity(msp.len());
    match_each_signed_integer_ptype!(msp.ptype(), |P| {
        for (row, value) in msp.as_slice::<P>().iter().enumerate() {
            // The MSP occupies word `K`, the lower parts the `K` words beneath it most
            // significant first, and anything above word `K` is the MSP's sign.
            let mut words = sign_extended_words(i64::from(*value), K);
            for (i, part) in lower.iter().enumerate() {
                words[K - 1 - i] = part[row];
            }
            out.push(i256_from_words(words));
        }
    });
    out.freeze()
}

#[cfg(test)]
mod tests;
