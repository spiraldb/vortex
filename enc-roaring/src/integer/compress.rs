use croaring::Bitmap;
use log::warn;
use num_traits::NumCast;

use enc::array::primitive::{PrimitiveArray, PRIMITIVE_ENCODING};
use enc::array::Encoding;
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
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        warn!("Roaring integer compress");

        if !config.is_enabled(self.id()) {
            return None;
        }

        // Only support primitive enc arrays
        if array.encoding().id() != &PRIMITIVE_ENCODING {
            warn!("Skipping roaring int, not primitive");
            return None;
        }

        // Only support non-nullable uint arrays
        if !matches!(array.dtype(), DType::Int(_, Unsigned, NonNullable)) {
            warn!("Skipping roaring int, not non-nullable");
            return None;
        }

        // Only support sorted unique arrays
        if !array.stats().get_or_compute_or(false, &Stat::IsSorted) {
            return None;
        }
        if !array.stats().get_or_compute_or(false, &Stat::IsUnique) {
            return None;
        }

        // TODO(ngates): check that max is <= u32

        Some(&(roaring_int_compressor as Compressor))
    }
}

fn roaring_int_compressor(array: &dyn Array, _opts: CompressCtx) -> ArrayRef {
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
    values.iter().for_each(|&i| bitmap.add(i.to_u32().unwrap()));
    bitmap.run_optimize();
    // bitmap.shrink_to_fit();
    RoaringIntArray::new(bitmap)
}
