use vortex::array::PrimitiveArray;
use vortex::validity::Validity;
use vortex::IntoArray;
use vortex_dtype::{NativePType, PType};
use vortex_error::{vortex_bail, VortexResult};
use zigzag::ZigZag as ExternalZigZag;

use crate::ZigZagArray;

pub fn zigzag_encode(parray: &PrimitiveArray) -> VortexResult<ZigZagArray> {
    let encoded = match parray.ptype() {
        PType::I8 => zigzag_encode_primitive::<i8>(parray.maybe_null_slice(), parray.validity()),
        PType::I16 => zigzag_encode_primitive::<i16>(parray.maybe_null_slice(), parray.validity()),
        PType::I32 => zigzag_encode_primitive::<i32>(parray.maybe_null_slice(), parray.validity()),
        PType::I64 => zigzag_encode_primitive::<i64>(parray.maybe_null_slice(), parray.validity()),
        _ => vortex_bail!("Unsupported ptype {}", parray.ptype()),
    };
    ZigZagArray::try_new(encoded.into_array())
}

fn zigzag_encode_primitive<T: ExternalZigZag + NativePType>(
    values: &[T],
    validity: Validity,
) -> PrimitiveArray
where
    <T as ExternalZigZag>::UInt: NativePType,
{
    let mut encoded = Vec::with_capacity(values.len());
    encoded.extend(values.iter().map(|v| T::encode(*v)));
    PrimitiveArray::from_vec(encoded.to_vec(), validity)
}

pub fn zigzag_decode(parray: &PrimitiveArray) -> PrimitiveArray {
    match parray.ptype() {
        PType::U8 => zigzag_decode_primitive::<i8>(parray.maybe_null_slice(), parray.validity()),
        PType::U16 => zigzag_decode_primitive::<i16>(parray.maybe_null_slice(), parray.validity()),
        PType::U32 => zigzag_decode_primitive::<i32>(parray.maybe_null_slice(), parray.validity()),
        PType::U64 => zigzag_decode_primitive::<i64>(parray.maybe_null_slice(), parray.validity()),
        _ => panic!("Unsupported ptype {}", parray.ptype()),
    }
}

fn zigzag_decode_primitive<T: ExternalZigZag + NativePType>(
    values: &[T::UInt],
    validity: Validity,
) -> PrimitiveArray
where
    <T as ExternalZigZag>::UInt: NativePType,
{
    let mut encoded = Vec::with_capacity(values.len());
    encoded.extend(values.iter().map(|v| T::decode(*v)));
    PrimitiveArray::from_vec(encoded, validity)
}

#[cfg(test)]
mod test {
    use vortex::encoding::ArrayEncoding;

    use super::*;
    use crate::ZigZagEncoding;

    #[test]
    fn test_compress() {
        let compressed = zigzag_encode(&PrimitiveArray::from(Vec::from_iter(
            (-10_000..10_000).map(|i| i as i64),
        )))
        .unwrap();
        assert_eq!(compressed.array().encoding().id(), ZigZagEncoding.id());
    }
}
