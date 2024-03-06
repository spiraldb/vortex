use itertools::Itertools;
use std::ops::Deref;

use crate::array::chunked::{ChunkedArray, ChunkedEncoding};
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use crate::error::VortexResult;

impl EncodingCompression for ChunkedEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        (array.encoding().id() == &Self::ID).then_some(&(chunked_compressor as Compressor))
    }
}

fn chunked_compressor(
    array: &dyn Array,
    like: Option<&dyn Array>,
    ctx: CompressCtx,
) -> VortexResult<ArrayRef> {
    let chunked_array = array.as_chunked();
    let chunked_like = like.map(|like_array| like_array.as_chunked());

    let compressed_chunks = chunked_array
        .chunks()
        .iter()
        .enumerate()
        .map(|(i, chunk)| {
            let like_chunk = chunked_like
                .and_then(|c_like| c_like.chunks().get(i))
                .map(Deref::deref);
            ctx.compress(chunk.deref(), like_chunk)
        })
        .try_collect()?;

    Ok(ChunkedArray::new(compressed_chunks, array.dtype().clone()).boxed())
}
