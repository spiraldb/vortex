use crate::array::varbinview::VarBinViewArray;
use crate::array::{Array, ArrayRef};
use crate::compress::{ArrayCompression, CompressCtx};

impl ArrayCompression for VarBinViewArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        VarBinViewArray::new(
            ctx.compress(self.views()),
            self.data()
                .iter()
                .map(|d| ctx.compress(d.as_ref()))
                .collect(),
            self.dtype().clone(),
            self.validity().map(|v| ctx.compress(v.as_ref())),
        )
        .boxed()
    }
}
