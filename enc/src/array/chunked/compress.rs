use crate::array::chunked::ChunkedArray;
use crate::array::{Array, ArrayRef};
use crate::compress::{ArrayCompression, CompressCtx};

impl ArrayCompression for ChunkedArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        ChunkedArray::new(
            self.chunks
                .iter()
                .map(|chunk| ctx.compress(chunk.as_ref()))
                .collect(),
            self.dtype.clone(),
        )
        .boxed()
    }
}
