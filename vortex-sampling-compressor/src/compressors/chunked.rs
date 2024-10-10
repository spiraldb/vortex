use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use log::warn;
use vortex::array::{Chunked, ChunkedArray};
use vortex::encoding::EncodingRef;
use vortex::{Array, ArrayDType, ArrayDef, IntoArray};
use vortex_error::{vortex_bail, VortexResult};

use super::EncoderMetadata;
use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct ChunkedCompressor {
    relatively_good_ratio: f32,
}

pub const DEFAULT_CHUNKED_COMPRESSOR: ChunkedCompressor = ChunkedCompressor {
    relatively_good_ratio: 1.2,
};

pub struct ChunkedCompressorMetadata(Option<f32>);

impl EncoderMetadata for ChunkedCompressorMetadata {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EncodingCompressor for ChunkedCompressor {
    fn id(&self) -> &str {
        Chunked::ID.as_ref()
    }

    fn cost(&self) -> u8 {
        0
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        ChunkedArray::try_from(array)
            .ok()
            .map(|_| self as &dyn EncodingCompressor)
    }

    fn compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        let chunked_array = ChunkedArray::try_from(array)?;
        let like_and_ratio = like_into_parts(like)?;
        self.compress_chunked(&chunked_array, like_and_ratio, ctx)
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([])
    }
}

fn like_into_parts(
    tree: Option<CompressionTree<'_>>,
) -> VortexResult<Option<(CompressionTree<'_>, f32)>> {
    let (_, mut children, metadata) = match tree {
        None => return Ok(None),
        Some(tree) => tree.into_parts(),
    };

    let Some(target_ratio) = metadata else {
        vortex_bail!("chunked array compression tree must have metadata")
    };

    let Some(ChunkedCompressorMetadata(target_ratio)) =
        target_ratio.as_ref().as_any().downcast_ref()
    else {
        vortex_bail!("chunked array compression tree must be ChunkedCompressorMetadata")
    };

    if children.len() != 1 {
        vortex_bail!("chunked array compression tree must have one child")
    }

    let child = children.remove(0);

    match (child, target_ratio) {
        (None, None) => Ok(None),
        (Some(child), Some(ratio)) => Ok(Some((child, *ratio))),
        (..) => vortex_bail!("chunked array compression tree must have a child iff it has a ratio"),
    }
}

impl ChunkedCompressor {
    /// How far the compression ratio is allowed to grow from one chunk to another chunk.
    ///
    /// As long as a compressor compresses subsequent chunks "reasonably well" we should continue to
    /// use it, which saves us the cost of searching for a good compressor. This constant quantifies
    /// "reasonably well" as
    ///
    /// ```no_run
    /// new_ratio <= old_ratio * self.relatively_good_ratio
    /// ```
    fn relatively_good_ratio(&self) -> f32 {
        self.relatively_good_ratio
    }

    fn compress_chunked<'a>(
        &'a self,
        array: &ChunkedArray,
        mut previous: Option<(CompressionTree<'a>, f32)>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        let less_chunked = array.rechunk(
            ctx.options().target_block_bytesize,
            ctx.options().target_block_size,
        )?;
        let mut compressed_chunks = Vec::with_capacity(less_chunked.nchunks());
        for (index, chunk) in less_chunked.chunks().enumerate() {
            let like = previous.as_ref().map(|(like, _)| like);
            let (compressed_chunk, tree) = ctx
                .named(&format!("chunk-{}", index))
                .compress(&chunk, like)?
                .into_parts();

            let ratio = (compressed_chunk.nbytes() as f32) / (chunk.nbytes() as f32);
            let exceeded_target_ratio = previous
                .as_ref()
                .map(|(_, target_ratio)| ratio > target_ratio * self.relatively_good_ratio())
                .unwrap_or(false);

            if ratio > 1.0 || exceeded_target_ratio {
                warn!("unsatisfactory ratio {} {:?}", ratio, previous);
                let (compressed_chunk, tree) = ctx.compress_array(&chunk)?.into_parts();
                let new_ratio = (compressed_chunk.nbytes() as f32) / (chunk.nbytes() as f32);
                previous = tree.map(|tree| (tree, new_ratio));
                compressed_chunks.push(compressed_chunk);
            } else {
                previous = previous.or_else(|| tree.map(|tree| (tree, ratio)));
                compressed_chunks.push(compressed_chunk);
            }
        }

        let (child, ratio) = match previous {
            Some((child, ratio)) => (Some(child), Some(ratio)),
            None => (None, None),
        };

        Ok(CompressedArray::new(
            ChunkedArray::try_new(compressed_chunks, array.dtype().clone())?.into_array(),
            Some(CompressionTree::new_with_metadata(
                self,
                vec![child],
                Arc::new(ChunkedCompressorMetadata(ratio)),
            )),
        ))
    }
}
