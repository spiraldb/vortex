use itertools::Itertools;
use num_traits::{Float, NumCast, PrimInt};
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::ptype::NativePType;

use vortex::array::{Array, ArrayRef};

const SAMPLE_SIZE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exponents {
    pub e: u8,
    pub f: u8,
}

pub trait ALPFloat: NativePType + Float {
    type ALPInt: NativePType + PrimInt;
    const FRACTIONAL_BITS: u8;
    const MAX_EXPONENT: u8;
    const SWEET: Self;
    const F10: &'static [Self];
    const IF10: &'static [Self];

    /// Round to the nearest floating integer by shifting in and out of the low precision range.
    fn fast_round(self) -> Self {
        (self + Self::SWEET) - Self::SWEET
    }

    fn as_int(self) -> Option<Self::ALPInt> {
        <Self::ALPInt as NumCast>::from(self)
    }

    fn find_best_exponents(values: &[Self]) -> Exponents {
        let mut best_e: u8 = 0;
        let mut best_f: u8 = 0;
        let mut best_nbytes: usize = usize::MAX;

        let sample = (values.len() > SAMPLE_SIZE).then(|| {
            values
                .iter()
                .step_by(values.len() / SAMPLE_SIZE)
                .cloned()
                .collect_vec()
        });

        // TODO(wmanning): idea, start with highest e, then find the best f
        // after that, try e's in descending order, with a gap no larger than the original e - f
        for e in 0..Self::MAX_EXPONENT {
            for f in 0..e {
                let (_, encoded, patches) = Self::encode_to_array(
                    sample.as_deref().unwrap_or(values),
                    Some(&Exponents { e, f }),
                );
                let size = encoded.nbytes() + patches.map_or(0, |p| p.nbytes());
                if size < best_nbytes {
                    best_nbytes = size;
                    best_e = e;
                    best_f = f;
                } else if size == best_nbytes && e - f < best_e - best_f {
                    best_e = e;
                    best_f = f;
                }
            }
        }

        Exponents {
            e: best_e,
            f: best_f,
        }
    }

    fn encode_to_array(
        values: &[Self],
        exponents: Option<&Exponents>,
    ) -> (Exponents, ArrayRef, Option<ArrayRef>) {
        let best_exponents =
            exponents.map_or_else(|| Self::find_best_exponents(values), Exponents::clone);
        let (values, exc_pos, exc) = Self::encode(values, &best_exponents);
        let len = values.len();
        (
            best_exponents,
            PrimitiveArray::from_vec(values).boxed(),
            (exc.len() > 0).then(|| {
                SparseArray::new(
                    PrimitiveArray::from_vec(exc_pos).boxed(),
                    PrimitiveArray::from_vec(exc).boxed(),
                    len,
                )
                .boxed()
            }),
        )
    }

    fn encode(values: &[Self], exponents: &Exponents) -> (Vec<Self::ALPInt>, Vec<u64>, Vec<Self>) {
        let mut exc_pos = Vec::new();
        let mut exc_value = Vec::new();
        let mut prev = Self::ALPInt::default();
        let encoded = values
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let encoded =
                    (*v * Self::F10[exponents.e as usize] * Self::IF10[exponents.f as usize])
                        .fast_round();
                let decoded =
                    encoded * Self::F10[exponents.f as usize] * Self::IF10[exponents.e as usize];

                if decoded == *v {
                    if let Some(e) = encoded.as_int() {
                        prev = e;
                        return e;
                    }
                }

                exc_pos.push(i as u64);
                exc_value.push(*v);
                // Emit the last known good value. This helps with run-end encoding.
                prev
            })
            .collect_vec();

        (encoded, exc_pos, exc_value)
    }

    fn decode_single(encoded: Self::ALPInt, exponents: &Exponents) -> Self {
        let encoded_float: Self = Self::from(encoded).unwrap();
        encoded_float * Self::F10[exponents.f as usize] * Self::IF10[exponents.e as usize]
    }
}

impl ALPFloat for f32 {
    type ALPInt = i32;
    const FRACTIONAL_BITS: u8 = 23;
    const MAX_EXPONENT: u8 = 10;
    const SWEET: Self =
        (1 << Self::FRACTIONAL_BITS) as Self + (1 << Self::FRACTIONAL_BITS - 1) as Self;

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
    ];
}

impl ALPFloat for f64 {
    type ALPInt = i64;
    const FRACTIONAL_BITS: u8 = 52;
    const MAX_EXPONENT: u8 = 18; // 10^18 is the maximum i64
    const SWEET: Self =
        (1u64 << Self::FRACTIONAL_BITS) as Self + (1u64 << Self::FRACTIONAL_BITS - 1) as Self;
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
        100000000000000000000000.0,
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
        0.00000000000000000000001,
    ];
}
