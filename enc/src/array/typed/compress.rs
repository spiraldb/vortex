use crate::array::typed::{TypedArray, TypedEncoding, TYPED_ENCODING};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};

impl EncodingCompression for TypedEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() == &TYPED_ENCODING {
            Some(&(typed_compressor as Compressor))
        } else {
            None
        }
    }
}

fn typed_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let typed_array = array.as_any().downcast_ref::<TypedArray>().unwrap();
    let typed_like =
        like.map(|like_array| like_array.as_any().downcast_ref::<TypedArray>().unwrap());

    TypedArray::new(
        ctx.compress(
            typed_array.untyped_array(),
            typed_like.map(|typed_arr| typed_arr.untyped_array()),
        ),
        array.dtype().clone(),
    )
    .boxed()
}
