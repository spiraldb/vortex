use crate::array::chunked::ChunkedArray;
use crate::array::{Array, ArrayRef};
use crate::compress::{ArrayCompression, CompressCtx};
use rayon::prelude::*;

impl ArrayCompression for ChunkedArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        ChunkedArray::new(
            self.chunks
                .par_iter()
                .map(|chunk| ctx.compress(chunk.as_ref()))
                .collect(),
            self.dtype.clone(),
        )
        .boxed()
    }
}
