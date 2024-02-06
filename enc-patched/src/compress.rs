use crate::PatchedArray;
use enc::array::{Array, ArrayRef};
use enc::compress::{ArrayCompression, CompressCtx};

impl ArrayCompression for PatchedArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        PatchedArray::new(
            ctx.compress(self.data().as_ref()),
            ctx.compress(self.patch_indices().as_ref()),
            ctx.compress(self.patch_values().as_ref()),
        )
        .boxed()
    }
}
