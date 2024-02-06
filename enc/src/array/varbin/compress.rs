use crate::array::varbin::VarBinArray;
use crate::array::{Array, ArrayRef};
use crate::compress::{ArrayCompression, CompressCtx};

impl ArrayCompression for VarBinArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        VarBinArray::new(
            ctx.compress(self.offsets()),
            ctx.compress(self.bytes()),
            self.dtype().clone(),
            self.validity().map(|v| ctx.compress(v.as_ref())),
        )
        .boxed()
    }
}
