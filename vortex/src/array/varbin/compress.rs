use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::varbin::{VarBinArray, VarBinEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};

impl EncodingCompression for VarBinEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() == &Self::ID {
            Some(&(varbin_compressor as Compressor))
        } else {
            None
        }
    }
}

fn varbin_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let varbin_array = array.as_varbin();
    let varbin_like = like.map(|like_array| like_array.as_varbin());

    VarBinArray::new(
        ctx.compress(
            varbin_array.offsets(),
            varbin_like.map(|typed_arr| typed_arr.offsets()),
        ),
        ctx.compress(
            varbin_array.bytes(),
            varbin_like.map(|typed_arr| typed_arr.bytes()),
        ),
        array.dtype().clone(),
        varbin_array.validity().map(|v| {
            ctx.compress(
                v.as_ref(),
                varbin_like
                    .and_then(|vblike| vblike.validity())
                    .map(|v| v.as_ref()),
            )
        }),
    )
    .boxed()
}
