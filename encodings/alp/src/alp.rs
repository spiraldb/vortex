use std::fmt::{Display, Formatter};
use std::mem::size_of;

use itertools::Itertools;
use num_traits::{Bounded, CheckedSub, Float, NumCast, PrimInt, ToPrimitive, Zero};
use serde::{Deserialize, Serialize};
use vortex_error::vortex_panic;

const SAMPLE_SIZE: usize = 32;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exponents {
    pub e: u8,
    pub f: u8,
}

impl Display for Exponents {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "e: {}, f: {}", self.e, self.f)
    }
}

pub trait ALPFloat: Float + Display + 'static {
    type ALPInt: PrimInt + Bounded + Display + ToPrimitive;

    const FRACTIONAL_BITS: u8;
    const MAX_EXPONENT: u8;
    const SWEET: Self;
    const F10: &'static [Self];
    const IF10: &'static [Self];

    /// Round to the nearest floating integer by shifting in and out of the low precision range.
    #[inline]
    fn fast_round(self) -> Self {
        (self + Self::SWEET) - Self::SWEET
    }

    #[inline]
    fn as_int(self) -> Option<Self::ALPInt> {
        <Self::ALPInt as NumCast>::from(self)
    }

    fn find_best_exponents(values: &[Self]) -> Exponents {
        let mut best_exp = Exponents { e: 0, f: 0 };
        let mut best_nbytes: usize = usize::MAX;

        let sample = (values.len() > SAMPLE_SIZE).then(|| {
            values
                .iter()
                .step_by(values.len() / SAMPLE_SIZE)
                .cloned()
                .collect_vec()
        });

        for e in (0..Self::MAX_EXPONENT).rev() {
            for f in 0..e {
                let (_, encoded, _, exc_patches) = Self::encode(
                    sample.as_deref().unwrap_or(values),
                    Some(Exponents { e, f }),
                );

                let size = Self::estimate_encoded_size(&encoded, &exc_patches);
                if size < best_nbytes {
                    best_nbytes = size;
                    best_exp = Exponents { e, f };
                } else if size == best_nbytes && e - f < best_exp.e - best_exp.f {
                    best_exp = Exponents { e, f };
                }
            }
        }

        best_exp
    }

    #[inline(always)]
    fn estimate_encoded_size(encoded: &[Self::ALPInt], patches: &[Self]) -> usize {
        let bits_per_encoded = encoded
            .iter()
            .minmax()
            .into_option()
            // estimating bits per encoded value assuming frame-of-reference + bitpacking-without-patches
            .and_then(|(min, max)| max.checked_sub(min))
            .and_then(|range_size: <Self as ALPFloat>::ALPInt| range_size.to_u64())
            .and_then(|range_size| {
                range_size
                    .checked_ilog2()
                    .map(|bits| (bits + 1) as usize)
                    .or(Some(0))
            })
            .unwrap_or(size_of::<Self::ALPInt>() * 8);

        let encoded_bytes = (encoded.len() * bits_per_encoded + 7) / 8;
        // each patch is a value + a position
        // in practice, patch positions are in [0, u16::MAX] because of how we chunk
        let patch_bytes = patches.len() * (size_of::<Self>() + size_of::<u16>());

        encoded_bytes + patch_bytes
    }

    fn encode(
        values: &[Self],
        exponents: Option<Exponents>,
    ) -> (Exponents, Vec<Self::ALPInt>, Vec<u64>, Vec<Self>) {
        let exp = exponents.unwrap_or_else(|| Self::find_best_exponents(values));

        let mut encoded_output = Vec::with_capacity(values.len());
        let mut patch_indices = Vec::new();
        let mut patch_values = Vec::new();
        let mut fill_value: Option<Self::ALPInt> = None;
        let mut has_filled = false;

        // this is intentionally branchless
        // TODO: batch this into 1024 values at a time to make it more cache friendly
        const CHUNK_SIZE: usize = 1024;
        for (chunk_idx, chunk) in values.chunks(CHUNK_SIZE).enumerate() {
            let mut chunk_patch_count = 0;
            encoded_output.extend(chunk
                .iter()
                .map(|v| {
                    let encoded = unsafe { Self::encode_single_unchecked(*v, exp) };
                    let decoded = Self::decode_single(encoded, exp);
                    let neq: usize = (decoded != *v) as usize;
                    chunk_patch_count += neq;
                    encoded
                }));
            let chunk_patch_count = chunk_patch_count; // immutable hereafter

            if chunk_patch_count > 0 {
                let num_prev_encoded = chunk_idx * CHUNK_SIZE;
                let num_prev_patches = patch_indices.len();

                let mut patch_indices_mut = patch_indices.spare_capacity_mut();
                let mut patch_values_mut = patch_values.spare_capacity_mut();

                let mut chunk_patch_index = 0;
                for i in num_prev_encoded..encoded_output.len() {
                    let decoded = Self::decode_single(encoded_output[i], exp);
                    patch_indices_mut[chunk_patch_index] = i as u64;
                    patch_values_mut[chunk_patch_index] = values[i];
                    chunk_patch_index += (decoded != values[i]) as usize;
                }
                assert_eq!(chunk_patch_index, chunk_patch_count);
                unsafe {
                    patch_indices.set_len(num_prev_patches + chunk_patch_count);
                    patch_values.set_len(num_prev_patches + chunk_patch_count);
                }

                // find the first successfully encoded value (i.e., not patched)
                if fill_value.is_none() {
                    assert_eq!(num_prev_encoded, num_prev_patches);
                    for i in num_prev_encoded..encoded_output.len() {
                        if patch_indices[i] != i as u64 {
                            fill_value = Some(encoded_output[i]);
                            break;
                        }
                    }
                }

                if let Some(fill_value) = fill_value {
                    let patch_indices_to_fill = if !has_filled {
                        &patch_indices 
                    } else {
                        &patch_indices[num_prev_patches..]
                    };

                    for patch_idx in patch_indices_to_fill.iter() {
                        encoded_output[*patch_idx as usize] = fill_value;
                    }
                    has_filled = true;
                }
            }
        }

        (exp, encoded_output, patch_indices, patch_values)
    }

    #[inline]
    fn encode_single(value: Self, exponents: Exponents) -> Result<Self::ALPInt, Self> {
        let encoded = unsafe { Self::encode_single_unchecked(value, exponents) };
        let decoded = Self::decode_single(encoded, exponents);
        if decoded == value {
            return Ok(encoded);
        }
        Err(value)
    }

    #[inline]
    fn decode_single(encoded: Self::ALPInt, exponents: Exponents) -> Self {
        let encoded_float: Self = Self::from(encoded).unwrap_or_else(|| {
            vortex_panic!(
                "Failed to convert encoded value {} from {} to {} in ALPFloat::decode_single",
                encoded,
                std::any::type_name::<Self::ALPInt>(),
                std::any::type_name::<Self>()
            )
        });
        encoded_float * Self::F10[exponents.f as usize] * Self::IF10[exponents.e as usize]
    }

    /// # Safety
    ///
    /// The returned value may not decode back to the original value.
    #[inline(always)]
    unsafe fn encode_single_unchecked(value: Self, exponents: Exponents) -> Self::ALPInt {
        (value * Self::F10[exponents.e as usize] * Self::IF10[exponents.f as usize])
            .fast_round()
            .as_int()
            .unwrap_or_else(Self::ALPInt::max_value)
    }
}

impl ALPFloat for f32 {
    type ALPInt = i32;
    const FRACTIONAL_BITS: u8 = 23;
    const MAX_EXPONENT: u8 = 10;
    const SWEET: Self =
        (1 << Self::FRACTIONAL_BITS) as Self + (1 << (Self::FRACTIONAL_BITS - 1)) as Self;

    const F10: &'static [Self] = &[
        1.0,
        10.0,
        100.0,
        1000.0,
        10000.0,
        100000.0,
        1000000.0,
        10000000.0,
        100000000.0,
        1000000000.0,
        10000000000.0, // 10^10
    ];
    const IF10: &'static [Self] = &[
        1.0,
        0.1,
        0.01,
        0.001,
        0.0001,
        0.00001,
        0.000001,
        0.0000001,
        0.00000001,
        0.000000001,
        0.0000000001, // 10^-10
    ];
}

impl ALPFloat for f64 {
    type ALPInt = i64;
    const FRACTIONAL_BITS: u8 = 52;
    const MAX_EXPONENT: u8 = 18; // 10^18 is the maximum i64
    const SWEET: Self =
        (1u64 << Self::FRACTIONAL_BITS) as Self + (1u64 << (Self::FRACTIONAL_BITS - 1)) as Self;
    const F10: &'static [Self] = &[
        1.0,
        10.0,
        100.0,
        1000.0,
        10000.0,
        100000.0,
        1000000.0,
        10000000.0,
        100000000.0,
        1000000000.0,
        10000000000.0,
        100000000000.0,
        1000000000000.0,
        10000000000000.0,
        100000000000000.0,
        1000000000000000.0,
        10000000000000000.0,
        100000000000000000.0,
        1000000000000000000.0,
        10000000000000000000.0,
        100000000000000000000.0,
        1000000000000000000000.0,
        10000000000000000000000.0,
        100000000000000000000000.0, // 10^23
    ];

    const IF10: &'static [Self] = &[
        1.0,
        0.1,
        0.01,
        0.001,
        0.0001,
        0.00001,
        0.000001,
        0.0000001,
        0.00000001,
        0.000000001,
        0.0000000001,
        0.00000000001,
        0.000000000001,
        0.0000000000001,
        0.00000000000001,
        0.000000000000001,
        0.0000000000000001,
        0.00000000000000001,
        0.000000000000000001,
        0.0000000000000000001,
        0.00000000000000000001,
        0.000000000000000000001,
        0.0000000000000000000001,
        0.00000000000000000000001, // 10^-23
    ];
}
