use polars_arrow::legacy::trusted_len::TrustedLenPush;
use zigzag::ZigZag;

use crate::array::primitive::PrimitiveArray;
use crate::array::stats::Stat;
use crate::array::zigzag::{ZigZagArray, ZigZagEncoding};
use crate::array::{Array, ArrayKind, ArrayRef, Encoding};
use crate::compute::compress::{
    compress, CompressConfig, CompressCtx, CompressedEncoding, Compressible, Compressor,
};
use crate::types::{NativePType, PType};

impl Compressible for ZigZagArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        // Recursively compress the inner encoded array.
        compress(self.encoded(), ctx.next_level())
    }
}

impl CompressedEncoding for ZigZagEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if !config.is_enabled(self.id()) {
            return None;
        }

        // Only support primitive arrays
        let Some(parray) = array.as_any().downcast_ref::<PrimitiveArray>() else {
            return None;
        };

        // Only supports signed integers
        if !parray.ptype().is_signed_int() {
            return None;
        }

        // Only compress if the array has negative values
        match parray.stats().get_or_compute_cast::<i64>(&Stat::Min) {
            None => {
                // Unknown whether the array has negative values?
                return None;
            }
            Some(min_scalar) => {
                if min_scalar >= 0 {
                    return None;
                }
            }
        }

        // TODO(ngates): also check that Stat::Max is less than half the max value of the type

        Some(&(zigzag_compressor as Compressor))
    }
}

fn zigzag_compressor(array: &dyn Array, opts: CompressCtx) -> ArrayRef {
    let encoded = match ArrayKind::from(array) {
        ArrayKind::Primitive(p) => zigzag_encode(p),
        _ => panic!("Compress more arrays"),
    };
    compress(encoded.as_ref(), opts)
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
    unsafe { encoded.extend_trusted_len_unchecked(values.iter().map(|v| T::encode(*v))) };
    PrimitiveArray::from_vec(encoded)
}

pub fn zigzag_decode(parray: &PrimitiveArray) -> PrimitiveArray {
    match parray.ptype() {
        PType::U8 => zigzag_decode_primitive::<i8>(parray.buffer().typed_data()),
        PType::U16 => zigzag_decode_primitive::<i16>(parray.buffer().typed_data()),
        PType::U32 => zigzag_decode_primitive::<i32>(parray.buffer().typed_data()),
        PType::U64 => zigzag_decode_primitive::<i64>(parray.buffer().typed_data()),
        _ => panic!("Unsupported ptype"),
    }
}

fn zigzag_decode_primitive<T: ZigZag + NativePType>(values: &[T::UInt]) -> PrimitiveArray
where
    <T as ZigZag>::UInt: NativePType,
{
    let encoded: Vec<T> = values.iter().map(|v| T::decode(*v)).collect();
    PrimitiveArray::from_vec(encoded)
}
