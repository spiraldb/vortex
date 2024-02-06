use crate::array::struct_::StructArray;
use crate::array::{Array, ArrayRef};
use crate::compress::{ArrayCompression, CompressCtx};

impl ArrayCompression for StructArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        StructArray::new(
            self.names().clone(),
            self.fields()
                .iter()
                .map(|f| ctx.compress(f.as_ref()))
                .collect(),
        )
        .boxed()
    }
}
