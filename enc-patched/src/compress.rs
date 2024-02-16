use enc::array::{Array, ArrayRef};
use enc::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};

use crate::{PatchedArray, PatchedEncoding};

impl EncodingCompression for PatchedEncoding {
    fn compressor(
        &self,
        _array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        Some(&(patched_compressor as Compressor))
    }
}

// TODO(robert): Transpose patched arrays so that this is unnecessary
fn patched_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let patched_like = like.map(|like_arr| {
        like_arr
            .as_any()
            .downcast_ref::<PatchedArray>()
            .unwrap()
            .data()
    });

    ctx.compress(array, patched_like)
}
