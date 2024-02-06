use crate::array::bool::BoolArray;
use crate::array::ArrayRef;
use crate::compress::{sampled_compression, ArrayCompression, CompressCtx};

impl ArrayCompression for BoolArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        sampled_compression(self, ctx)
    }
}
