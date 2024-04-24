use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::ptype::{NativePType, PType};
use vortex::stats::{ArrayStatistics, Stat};
use vortex::validity::Validity;
use vortex::{Array, ArrayDef, AsArray, IntoArray, OwnedArray};
use vortex_alloc::{AlignedVec, ALIGNED_ALLOCATOR};
use vortex_error::VortexResult;
use zigzag::ZigZag as ExternalZigZag;

use crate::{OwnedZigZagArray, ZigZagEncoding};

impl EncodingCompression for ZigZagEncoding {
    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = PrimitiveArray::try_from(array).ok()?;

        // Only supports signed integers
        if !parray.ptype().is_signed_int() {
            return None;
        }

        // Only compress if the array has negative values
        // TODO(ngates): also check that Stat::Max is less than half the max value of the type
        parray
            .statistics()
            .compute_as_cast::<i64>(Stat::Min)
            .filter(|&min| min < 0)
            .map(|_| self as &dyn EncodingCompression)
    }

    fn compress(
        &self,
        array: &Array,
        like: Option<&Array>,
        ctx: CompressCtx,
    ) -> VortexResult<OwnedArray> {
        let zigzag_like = like.map(|like_arr| like_arr.as_array_ref());
        let encoded = if array.encoding().id() == Primitive::ID {
            zigzag_encode(&array.as_primitive())?
        } else {
            println!("{}", array.encoding().id());
            unreachable!("This array kind should have been filtered out")
        };

        Ok(OwnedZigZagArray::new(ctx.compress(&encoded.encoded(), zigzag_like)?).into_array())
    }
}

pub fn zigzag_encode(parray: &PrimitiveArray<'_>) -> VortexResult<OwnedZigZagArray> {
    let encoded = match parray.ptype() {
        PType::I8 => {
            zigzag_encode_primitive::<i8>(parray.buffer().typed_data(), Some(parray.validity()))
        }
        PType::I16 => {
            zigzag_encode_primitive::<i16>(parray.buffer().typed_data(), Some(parray.validity()))
        }
        PType::I32 => {
            zigzag_encode_primitive::<i32>(parray.buffer().typed_data(), Some(parray.validity()))
        }
        PType::I64 => {
            zigzag_encode_primitive::<i64>(parray.buffer().typed_data(), Some(parray.validity()))
        }
        _ => panic!("Unsupported ptype {}", parray.ptype()),
    };
    OwnedZigZagArray::try_new(encoded.into_array())
}

fn zigzag_encode_primitive<'a, T: ExternalZigZag + NativePType>(
    values: &'a [T],
    validity: Option<Validity<'a>>,
) -> PrimitiveArray<'a>
where
    <T as ExternalZigZag>::UInt: NativePType,
{
    let mut encoded = AlignedVec::with_capacity_in(values.len(), ALIGNED_ALLOCATOR);
    encoded.extend(values.iter().map(|v| T::encode(*v)));
    PrimitiveArray::from_vec(encoded.to_vec(), validity.to_owned().unwrap())
}

#[allow(dead_code)]
pub fn zigzag_decode<'a>(parray: &'a PrimitiveArray<'a>) -> PrimitiveArray<'a> {
    match parray.ptype() {
        PType::U8 => {
            zigzag_decode_primitive::<i8>(parray.buffer().typed_data(), Some(parray.validity()))
        }
        PType::U16 => {
            zigzag_decode_primitive::<i16>(parray.buffer().typed_data(), Some(parray.validity()))
        }
        PType::U32 => {
            zigzag_decode_primitive::<i32>(parray.buffer().typed_data(), Some(parray.validity()))
        }
        PType::U64 => {
            zigzag_decode_primitive::<i64>(parray.buffer().typed_data(), Some(parray.validity()))
        }
        _ => panic!("Unsupported ptype {}", parray.ptype()),
    }
}

#[allow(dead_code)]
fn zigzag_decode_primitive<'a, T: ExternalZigZag + NativePType>(
    values: &'a [T::UInt],
    validity: Option<Validity<'a>>,
) -> PrimitiveArray<'a>
where
    <T as ExternalZigZag>::UInt: NativePType,
{
    let mut encoded: AlignedVec<T> = AlignedVec::with_capacity_in(values.len(), ALIGNED_ALLOCATOR);
    encoded.extend(values.iter().map(|v| T::decode(*v)));
    if let Some(valid) = validity {
        PrimitiveArray::from_vec(encoded.to_vec(), valid)
    } else {
        PrimitiveArray::from(encoded.to_vec())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::encoding::{ArrayEncoding, EncodingRef};
    use vortex_fastlanes::BitPackedEncoding;

    use super::*;

    #[test]
    fn test_compress() {
        let cfg = CompressConfig::new()
            .with_enabled([&ZigZagEncoding as EncodingRef, &BitPackedEncoding])
            .with_disabled(vec![]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let compressed = ctx
            .compress(
                PrimitiveArray::from(Vec::from_iter((-10_000..10_000).map(|i| i as i64))).array(),
                None,
            )
            .unwrap();
        assert_eq!(compressed.encoding().id(), ZigZagEncoding.id());
    }
}
