use croaring::Bitmap;

use enc::array::bool::{BoolArray, BOOL_ENCODING};
use enc::array::{Array, ArrayRef};
use enc::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use enc::dtype::DType;
use enc::dtype::Nullability::NonNullable;

use crate::boolean::{RoaringBoolArray, RoaringBoolEncoding};

impl EncodingCompression for RoaringBoolEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support bool enc arrays
        if array.encoding().id() != &BOOL_ENCODING {
            return None;
        }

        // Only support non-nullable bool arrays
        if array.dtype() != &DType::Bool(NonNullable) {
            return None;
        }

        if array.len() > u32::MAX as usize {
            return None;
        }

        Some(&(roaring_compressor as Compressor))
    }
}

fn roaring_compressor(array: &dyn Array, _like: Option<&dyn Array>, _ctx: CompressCtx) -> ArrayRef {
    roaring_encode(array.as_any().downcast_ref::<BoolArray>().unwrap()).boxed()
}

pub fn roaring_encode(bool_array: &BoolArray) -> RoaringBoolArray {
    let mut bitmap = Bitmap::new();
    bitmap.extend(
        bool_array
            .buffer()
            .iter()
            .enumerate()
            .filter(|(_, b)| *b)
            .map(|(i, _)| i as u32),
    );
    bitmap.run_optimize();

    RoaringBoolArray::new(bitmap, bool_array.buffer().len())
}
