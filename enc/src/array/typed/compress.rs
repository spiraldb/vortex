use crate::array::typed::TypedArray;
use crate::array::{Array, ArrayRef};
use crate::compress::{ArrayCompression, CompressCtx};

impl ArrayCompression for TypedArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        TypedArray::new(ctx.compress(self.untyped_array()), self.dtype().clone()).boxed()
    }
}
