use itertools::Itertools;
use num_traits::{PrimInt, WrappingAdd, WrappingSub};
use vortex::array::constant::ConstantArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::{CompressConfig, Compressor, EncodingCompression};
use vortex::stats::{ArrayStatistics, Stat};
use vortex::validity::ArrayValidity;
use vortex::{Array, ArrayDType, ArrayTrait, IntoArray, IntoArrayVariant};
use vortex_dtype::{match_each_integer_ptype, NativePType};
use vortex_error::{vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::{FoRArray, FoREncoding};

impl EncodingCompression for FoREncoding {
    fn cost(&self) -> u8 {
        0
    }

    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = PrimitiveArray::try_from(array).ok()?;

        // Only supports integers
        if !parray.ptype().is_int() {
            return None;
        }

        // For all-null, cannot encode.
        if parray.logical_validity().all_invalid() {
            return None;
        }

        // Nothing for us to do if the min is already zero and tz == 0
        let shift = trailing_zeros(array);
        let min = parray.statistics().compute_as_cast::<i64>(Stat::Min)?;
        if min == 0 && shift == 0 {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &Array,
        like: Option<&Array>,
        ctx: Compressor,
    ) -> VortexResult<Array> {
        let parray = PrimitiveArray::try_from(array)?;
        let shift = trailing_zeros(array);
        let min = parray
            .statistics()
            .compute(Stat::Min)
            .ok_or_else(|| vortex_err!("Min stat not found"))?;

        let child = match_each_integer_ptype!(parray.ptype(), |$T| {
            if shift == <$T>::PTYPE.bit_width() as u8 {
                ConstantArray::new(
                    Scalar::zero::<$T>(parray.dtype().nullability())
                        .reinterpret_cast(parray.ptype().to_unsigned()),
                    parray.len(),
                )
                .into_array()
            } else {
                compress_primitive::<$T>(&parray, shift, $T::try_from(&min)?)
                    .reinterpret_cast(parray.ptype().to_unsigned())
                    .into_array()
            }
        });
        let for_like = like.map(|like_arr| FoRArray::try_from(like_arr).unwrap());

        let compressed_child = ctx
            .named("for")
            .excluding(&Self)
            .compress(&child, for_like.as_ref().map(|l| l.encoded()).as_ref())?;
        FoRArray::try_new(compressed_child, min, shift).map(|a| a.into_array())
    }
}

fn compress_primitive<T: NativePType + WrappingSub + PrimInt>(
    parray: &PrimitiveArray,
    shift: u8,
    min: T,
) -> PrimitiveArray {
    assert!(shift < T::PTYPE.bit_width() as u8);
    let values = if shift > 0 {
        let shifted_min = min >> shift as usize;
        parray
            .maybe_null_slice::<T>()
            .iter()
            .map(|&v| v >> shift as usize)
            .map(|v| v.wrapping_sub(&shifted_min))
            .collect_vec()
    } else {
        parray
            .maybe_null_slice::<T>()
            .iter()
            .map(|&v| v.wrapping_sub(&min))
            .collect_vec()
    };

    PrimitiveArray::from_vec(values, parray.validity())
}

pub fn decompress(array: FoRArray) -> VortexResult<PrimitiveArray> {
    let shift = array.shift();
    let ptype = array.ptype();
    let encoded = array.encoded().into_primitive()?.reinterpret_cast(ptype);
    Ok(match_each_integer_ptype!(ptype, |$T| {
        let reference: $T = array.reference().try_into()?;
        PrimitiveArray::from_vec(
            decompress_primitive(encoded.maybe_null_slice::<$T>(), reference, shift),
            encoded.validity(),
        )
    }))
}

fn decompress_primitive<T: NativePType + WrappingAdd + PrimInt>(
    values: &[T],
    reference: T,
    shift: u8,
) -> Vec<T> {
    if shift > 0 {
        let shifted_reference = reference << shift as usize;
        values
            .iter()
            .map(|&v| v << shift as usize)
            .map(|v| v.wrapping_add(&shifted_reference))
            .collect_vec()
    } else {
        values
            .iter()
            .map(|&v| v.wrapping_add(&reference))
            .collect_vec()
    }
}

fn trailing_zeros(array: &Array) -> u8 {
    let tz_freq = array
        .statistics()
        .compute_trailing_zero_freq()
        .unwrap_or_else(|| vec![0]);
    tz_freq
        .iter()
        .enumerate()
        .find_or_first(|(_, &v)| v > 0)
        .map(|(i, _)| i)
        .unwrap_or(0) as u8
}

#[cfg(test)]
mod test {
    use vortex::compute::unary::scalar_at::ScalarAtFn;
    use vortex::encoding::{ArrayEncoding, EncodingRef};
    use vortex::{Context, IntoArrayVariant};

    use super::*;
    use crate::BitPackedEncoding;

    fn ctx() -> Context {
        // We need some BitPacking else we will need choose FoR.
        Context::default().with_encodings([&FoREncoding as EncodingRef, &BitPackedEncoding])
    }

    #[test]
    fn test_compress() {
        // Create a range offset by a million
        let array = PrimitiveArray::from((0u32..10_000).map(|v| v + 1_000_000).collect_vec());

        let compressed = Compressor::new(&ctx())
            .compress(array.array(), None)
            .unwrap();
        assert_eq!(compressed.encoding().id(), FoREncoding.id());
        assert_eq!(
            u32::try_from(FoRArray::try_from(compressed).unwrap().reference()).unwrap(),
            1_000_000u32
        );
    }

    #[test]
    fn test_decompress() {
        // Create a range offset by a million
        let array = PrimitiveArray::from((0u32..10_000).map(|v| v + 1_000_000).collect_vec());
        let compressed = Compressor::new(&ctx())
            .compress(array.array(), None)
            .unwrap();
        assert_eq!(compressed.encoding().id(), FoREncoding.id());

        let decompressed = compressed.into_primitive().unwrap();
        assert_eq!(
            decompressed.maybe_null_slice::<u32>(),
            array.maybe_null_slice::<u32>()
        );
    }

    #[test]
    fn test_overflow() {
        let array = PrimitiveArray::from((i8::MIN..=i8::MAX).collect_vec());
        let compressed = FoREncoding
            .compress(array.array(), None, Compressor::new(&ctx()))
            .unwrap();
        let compressed = FoRArray::try_from(compressed).unwrap();
        assert_eq!(i8::MIN, i8::try_from(compressed.reference()).unwrap());

        let encoded = compressed.encoded().into_primitive().unwrap();
        let encoded_bytes: &[u8] = encoded.maybe_null_slice::<u8>();
        let unsigned: Vec<u8> = (0..=u8::MAX).collect_vec();
        assert_eq!(encoded_bytes, unsigned.as_slice());

        let decompressed = compressed.array().clone().into_primitive().unwrap();
        assert_eq!(
            decompressed.maybe_null_slice::<i8>(),
            array.maybe_null_slice::<i8>()
        );
        array
            .maybe_null_slice::<i8>()
            .iter()
            .enumerate()
            .for_each(|(i, v)| {
                assert_eq!(
                    *v,
                    i8::try_from(compressed.scalar_at(i).unwrap().as_ref()).unwrap()
                );
            });
    }
}
