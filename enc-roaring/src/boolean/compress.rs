use croaring::Bitmap;

use enc::array::bool::{BoolArray, BOOL_ENCODING};
use enc::array::Encoding;
use enc::array::{Array, ArrayRef};
use enc::compress::{
    ArrayCompression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};
use enc::dtype::DType;
use enc::dtype::Nullability::NonNullable;

use crate::boolean::{RoaringBoolArray, RoaringBoolEncoding};

impl ArrayCompression for RoaringBoolArray {
    fn compress(&self, _ctx: CompressCtx) -> ArrayRef {
        let mut bitmap = self.bitmap().clone();
        bitmap.run_optimize();
        RoaringBoolArray::new(bitmap, self.len()).boxed()
    }
}

impl EncodingCompression for RoaringBoolEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if !config.is_enabled(self.id()) {
            return None;
        }

        // Only support bool enc arrays
        if array.encoding().id() != &BOOL_ENCODING {
            return None;
        }

        // Only support non-nullable bool arrays
        if array.dtype() != &DType::Bool(NonNullable) {
            return None;
        }

        // TODO(ngates): check that max is <= u32

        Some(&(roaring_bool_compressor as Compressor))
    }
}

fn roaring_bool_compressor(array: &dyn Array, _opts: CompressCtx) -> ArrayRef {
    roaring_encode(array.as_any().downcast_ref::<BoolArray>().unwrap()).boxed()
}

pub fn roaring_encode(bool_array: &BoolArray) -> RoaringBoolArray {
    let mut bitmap = Bitmap::new();
    bool_array
        .buffer()
        .iter()
        .enumerate()
        .filter(|(_, b)| *b)
        .for_each(|(i, _)| bitmap.add(i as u32));
    bitmap.run_optimize();

    RoaringBoolArray::new(bitmap, bool_array.buffer().len())
}
