use crate::array::bool::{BoolEncoding, BOOL_ENCODING};
use crate::array::{Array, ArrayRef};
use crate::compress::{
    sampled_compression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};

impl EncodingCompression for BoolEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() == &BOOL_ENCODING {
            Some(&(bool_compressor as Compressor))
        } else {
            None
        }
    }
}

fn bool_compressor(array: &dyn Array, _like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    sampled_compression(array, ctx)
}
