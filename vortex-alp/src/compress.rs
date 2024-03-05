use itertools::Itertools;
use log::debug;
use num_traits::{cast, Float, PrimInt};

use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use vortex::ptype::{NativePType, PType};

use crate::alp::{ALPArray, ALPEncoding};
use crate::downcast::DowncastALP;
use crate::Exponents;

impl EncodingCompression for ALPEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support primitive arrays
        let Some(parray) = array.maybe_primitive() else {
            debug!("Skipping ALP: not primitive");
            return None;
        };

        // Only supports f32 and f64
        if !matches!(parray.ptype(), PType::F32 | PType::F64) {
            debug!("Skipping ALP: only supports f32 and f64");
            return None;
        }

        Some(&(alp_compressor as Compressor))
    }
}

fn alp_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let like_alp = like.map(|like_array| like_array.as_alp());

    let parray = array.as_primitive();
    let (exponents, encoded, patches) = match parray.ptype() {
        PType::F32 => {
            ALPFloat::encode_to_array(parray.typed_data::<f32>(), like_alp.map(|a| a.exponents()))
        }
        PType::F64 => {
            ALPFloat::encode_to_array(parray.typed_data::<f64>(), like_alp.map(|a| a.exponents()))
        }
        _ => panic!("Unsupported ptype"),
    };

    ALPArray::new(
        ctx.next_level()
            //.compress(encoded.as_ref(), like_alp.map(|a| a.encoded())),
            .compress(encoded.as_ref(), None),
        exponents,
        patches.map(|p| {
            ctx.next_level()
                //.compress(p.as_ref(), like_alp.and_then(|a| a.patches()))
                .compress(p.as_ref(), None)
        }),
    )
    .boxed()
}

pub fn alp_encode(parray: &PrimitiveArray) -> ALPArray {
    let (exponents, encoded, patches) = match parray.ptype() {
        PType::F32 => ALPFloat::encode_to_array(parray.typed_data::<f32>(), None),
        PType::F64 => ALPFloat::encode_to_array(parray.typed_data::<f64>(), None),
        _ => panic!("Unsupported ptype"),
    };
    ALPArray::new(encoded, exponents, patches)
}

trait ALPFloat: NativePType + Float {
    type ALPInt: NativePType + PrimInt;
    const FRACTIONAL_BITS: u8;
    const SWEET: Self;
    const F10: &'static [Self]; // TODO(ngates): const exprs for these to be arrays.
    const IF10: &'static [Self];

    /// Round to the nearest floating integer by shifting in and out of the low precision range.
    fn fast_round(self) -> Self {
        (self + Self::SWEET) - Self::SWEET
    }

    fn find_best_exponents(values: &[Self]) -> Exponents {
        let mut best_e: usize = 0;
        let mut best_f: usize = 0;
        let mut best_nbytes: usize = usize::MAX;

        // TODO(wmanning): idea, start with highest e, then find the best f
        // after that, try e's in descending order, with a gap no larger than the original e - f
        for e in 0..Self::F10.len() - 1 {
            for f in 0..e {
                let (_, encoded, patches) = Self::encode_to_array(
                    values,
                    Some(&Exponents {
                        e: e as u8,
                        f: f as u8,
                    }),
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
            e: best_e as u8,
            f: best_f as u8,
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
        let encoded = values
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let encoded =
                    (*v * Self::F10[exponents.e as usize] * Self::IF10[exponents.f as usize])
                        .fast_round();
                let decoded =
                    encoded * Self::F10[exponents.f as usize] * Self::IF10[exponents.e as usize];

                if decoded != *v {
                    exc_pos.push(i as u64);
                    exc_value.push(*v);
                    // TODO(ngates): we could find previous?
                    Self::default()
                } else {
                    *v
                }
            })
            .map(|v| cast(v).unwrap())
            .collect_vec();

        (encoded, exc_pos, exc_value)
    }
}

impl ALPFloat for f32 {
    type ALPInt = i32;
    const FRACTIONAL_BITS: u8 = 23;
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

//
// #[allow(dead_code)]
// pub fn alp_decode(parray: &PrimitiveArray, exp: ALPExponents) -> PrimitiveArray {
//     match parray.ptype() {
//         PType::I32 => PrimitiveArray::from_vec_in(
//             alp::decode::<f32>(parray.buffer().typed_data::<i32>(), exp).unwrap(),
//         ),
//         PType::I64 => PrimitiveArray::from_vec_in(
//             alp::decode::<f64>(parray.buffer().typed_data::<i64>(), exp).unwrap(),
//         ),
//         _ => panic!("Unsupported ptype"),
//     }
// }

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_compress() {
        // Create a range offset by a million
        let array = PrimitiveArray::from_vec(vec![1.234; 1024]);
        let encoded = alp_encode(&array);
        println!("Encoded {:?}", encoded);
        assert_eq!(encoded.patches(), None);
        assert_eq!(encoded.exponents(), &Exponents { e: 0, f: 0 });
    }
}
