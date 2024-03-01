use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::typed::{TypedArray, TypedEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};

impl EncodingCompression for TypedEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() == &Self::ID {
            Some(&(typed_compressor as Compressor))
        } else {
            None
        }
    }
}

fn typed_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let typed_array = array.as_typed();
    let typed_like = like.map(|like_array| like_array.as_typed());

    TypedArray::new(
        ctx.compress(
            typed_array.untyped_array(),
            typed_like.map(|typed_arr| typed_arr.untyped_array()),
        ),
        array.dtype().clone(),
    )
    .boxed()
}
