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
pub struct ChunkedCompressor;

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

fn like_into_parts<'a>(
    tree: Option<CompressionTree<'a>>,
) -> VortexResult<Option<(CompressionTree<'a>, f32)>> {
    match tree {
        None => Ok(None),
        Some(tree) => {
            let (_, mut children, metadata) = tree.into_parts();
            if let Some(target_ratio) = metadata {
                if let Some(ChunkedCompressorMetadata(target_ratio)) =
                    target_ratio.as_ref().as_any().downcast_ref()
                {
                    if children.len() == 1 {
                        match (children.remove(0), target_ratio) {
                            (Some(child), Some(ratio)) => Ok(Some((child, *ratio))),
                            (None, None) => Ok(None),
                            (..) => {
                                vortex_bail!("chunked array compression tree must have a child iff it has a ratio")
                            }
                        }
                    } else {
                        vortex_bail!("chunked array compression tree must have one child")
                    }
                } else {
                    vortex_bail!("chunked array compression tree must ChunkedCompressorMetadata")
                }
            } else {
                vortex_bail!("chunked array compression tree must have metadata")
            }
        }
    }
}

impl ChunkedCompressor {
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
                .map(|(_, target_ratio)| ratio > target_ratio * 1.2)
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
