use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::typed::{TypedArray, TypedEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use crate::error::VortexResult;

impl EncodingCompression for TypedEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        (array.encoding().id() == &Self::ID).then_some(&(typed_compressor as Compressor))
    }
}

fn typed_compressor(
    array: &dyn Array,
    like: Option<&dyn Array>,
    ctx: CompressCtx,
) -> VortexResult<ArrayRef> {
    let typed_array = array.as_typed();
    let typed_like = like.map(|like_array| like_array.as_typed());

    Ok(TypedArray::new(
        ctx.compress(
            typed_array.untyped_array(),
            typed_like.map(|typed_arr| typed_arr.untyped_array()),
        )?,
        array.dtype().clone(),
    )
    .boxed())
}
