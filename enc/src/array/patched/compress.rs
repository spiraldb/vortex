use crate::array::patched::PatchedArray;
use crate::array::{Array, ArrayRef};
use crate::compress::{ArrayCompression, CompressCtx};

impl ArrayCompression for PatchedArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        PatchedArray::new(
            ctx.compress(self.data.as_ref()),
            ctx.compress(self.patch_indices.as_ref()),
            ctx.compress(self.patch_values.as_ref()),
        )
        .boxed()
    }
}
