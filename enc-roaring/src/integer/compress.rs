use croaring::Bitmap;
use log::debug;
use num_traits::NumCast;

use enc::array::primitive::{PrimitiveArray, PRIMITIVE_ENCODING};
use enc::array::{Array, ArrayRef};
use enc::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use enc::dtype::DType;
use enc::dtype::Nullability::NonNullable;
use enc::dtype::Signedness::Unsigned;
use enc::ptype::{NativePType, PType};
use enc::stats::Stat;

use crate::{RoaringIntArray, RoaringIntEncoding};

impl EncodingCompression for RoaringIntEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support primitive enc arrays
        if array.encoding().id() != &PRIMITIVE_ENCODING {
            debug!("Skipping roaring int, not primitive");
            return None;
        }

        // Only support non-nullable uint arrays
        if !matches!(array.dtype(), DType::Int(_, Unsigned, NonNullable)) {
            debug!("Skipping roaring int, not non-nullable");
            return None;
        }

        // Only support sorted unique arrays
        if !array
            .stats()
            .get_or_compute_or(false, &Stat::IsStrictSorted)
        {
            debug!("Skipping roaring int, not strict sorted");
            return None;
        }

        if array.stats().get_or_compute_or(0usize, &Stat::Max) > u32::MAX as usize {
            debug!("Skipping roaring int, max is larger than {}", u32::MAX);
            return None;
        }

        debug!("Using roaring int");
        Some(&(roaring_int_compressor as Compressor))
    }
}

fn roaring_int_compressor(
    array: &dyn Array,
    _like: Option<&dyn Array>,
    _ctx: CompressCtx,
) -> ArrayRef {
    roaring_encode(array.as_any().downcast_ref::<PrimitiveArray>().unwrap()).boxed()
}

pub fn roaring_encode(primitive_array: &PrimitiveArray) -> RoaringIntArray {
    match primitive_array.ptype() {
        PType::U8 => roaring_encode_primitive::<u8>(primitive_array.buffer().typed_data()),
        PType::U16 => roaring_encode_primitive::<u16>(primitive_array.buffer().typed_data()),
        PType::U32 => roaring_encode_primitive::<u32>(primitive_array.buffer().typed_data()),
        PType::U64 => roaring_encode_primitive::<u64>(primitive_array.buffer().typed_data()),
        _ => panic!("Unsupported ptype"),
    }
}

fn roaring_encode_primitive<T: NumCast + NativePType>(values: &[T]) -> RoaringIntArray {
    let mut bitmap = Bitmap::new();
    bitmap.extend(values.iter().map(|i| i.to_u32().unwrap()));
    bitmap.run_optimize();
    RoaringIntArray::new(bitmap, T::PTYPE)
}
