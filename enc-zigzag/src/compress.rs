use crate::zigzag::{ZigZagArray, ZigZagEncoding};
use enc::array::primitive::PrimitiveArray;

use enc::array::{Array, ArrayKind, ArrayRef};
use enc::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use enc::ptype::{NativePType, PType};
use enc::stats::Stat;
use zigzag::ZigZag;

impl EncodingCompression for ZigZagEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support primitive arrays
        let Some(parray) = array.as_any().downcast_ref::<PrimitiveArray>() else {
            return None;
        };

        // Only supports signed integers
        if !parray.ptype().is_signed_int() {
            return None;
        }

        // Only compress if the array has negative values
        // TODO(ngates): also check that Stat::Max is less than half the max value of the type
        parray
            .stats()
            .get_or_compute_cast::<i64>(&Stat::Min)
            .filter(|min| min < &0)
            .map(|_| &(zigzag_compressor as Compressor))
    }
}

fn zigzag_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let zigzag_like = like.map(|like_arr| like_arr.as_any().downcast_ref::<ZigZagArray>().unwrap());
    let encoded = match ArrayKind::from(array) {
        ArrayKind::Primitive(p) => zigzag_encode(p),
        _ => unreachable!("This array kind should have been filtered out"),
    };

    ZigZagArray::new(
        ctx.next_level()
            .compress(encoded.as_ref(), zigzag_like.map(|z| z.encoded())),
    )
    .boxed()
}

pub fn zigzag_encode(parray: &PrimitiveArray) -> PrimitiveArray {
    match parray.ptype() {
        PType::I8 => zigzag_encode_primitive::<i8>(parray.buffer().typed_data()),
        PType::I16 => zigzag_encode_primitive::<i16>(parray.buffer().typed_data()),
        PType::I32 => zigzag_encode_primitive::<i32>(parray.buffer().typed_data()),
        PType::I64 => zigzag_encode_primitive::<i64>(parray.buffer().typed_data()),
        _ => panic!("Unsupported ptype"),
    }
}

fn zigzag_encode_primitive<T: ZigZag + NativePType>(values: &[T]) -> PrimitiveArray
where
    <T as ZigZag>::UInt: NativePType,
{
    let mut encoded = Vec::with_capacity(values.len());
    encoded.extend(values.iter().map(|v| T::encode(*v)));
    PrimitiveArray::from_vec(encoded)
}

#[allow(dead_code)]
pub fn zigzag_decode(parray: &PrimitiveArray) -> PrimitiveArray {
    match parray.ptype() {
        PType::U8 => zigzag_decode_primitive::<i8>(parray.buffer().typed_data()),
        PType::U16 => zigzag_decode_primitive::<i16>(parray.buffer().typed_data()),
        PType::U32 => zigzag_decode_primitive::<i32>(parray.buffer().typed_data()),
        PType::U64 => zigzag_decode_primitive::<i64>(parray.buffer().typed_data()),
        _ => panic!("Unsupported ptype"),
    }
}

#[allow(dead_code)]
fn zigzag_decode_primitive<T: ZigZag + NativePType>(values: &[T::UInt]) -> PrimitiveArray
where
    <T as ZigZag>::UInt: NativePType,
{
    let encoded: Vec<T> = values.iter().map(|v| T::decode(*v)).collect();
    PrimitiveArray::from_vec(encoded)
}
