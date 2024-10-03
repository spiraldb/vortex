use std::collections::HashSet;

use vortex::array::{Chunked, ChunkedArray};
use vortex::encoding::EncodingRef;
use vortex::{Array, ArrayDType, ArrayDef, IntoArray};
use vortex_error::{vortex_bail, VortexResult};

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct ChunkedCompressor;

impl ChunkedCompressor {
    fn compress_chunked<'a>(
        &'a self,
        array: &ChunkedArray,
        compress_child_like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        let mut target_ratio: Option<f32> = None;

        let less_chunked = array.rechunk(
            ctx.options().target_block_bytesize,
            ctx.options().target_block_size,
        )?;
        let mut compressed_chunks = Vec::with_capacity(less_chunked.nchunks());
        let mut previous = compress_child_like;
        for (index, chunk) in less_chunked.chunks().enumerate() {
            let compressed_chunk = ctx
                .named(&format!("chunk-{}", index))
                .compress(&chunk, previous.as_ref())?
                .into_array();

            let ratio = (compressed_chunk.nbytes() as f32) / (chunk.nbytes() as f32);
            if ratio > 1.0 || target_ratio.map(|r| ratio > r * 1.2).unwrap_or(false) {
                let (compressed_chunk, tree) = ctx.compress_array(&chunk)?.into_parts();
                previous = tree;
                target_ratio = Some((compressed_chunk.nbytes() as f32) / (chunk.nbytes() as f32));
                compressed_chunks.push(compressed_chunk);
            } else {
                compressed_chunks.push(compressed_chunk);
            }
        }
        Ok(CompressedArray::new(
            ChunkedArray::try_new(compressed_chunks, array.dtype().clone())?.into_array(),
            previous,
        ))
    }
}

impl EncodingCompressor for ChunkedCompressor {
    fn id(&self) -> &str {
        Chunked::ID.as_ref()
    }

    fn cost(&self) -> u8 {
        0
    }

    fn can_compress(&self, _array: &Array) -> Option<&dyn EncodingCompressor> {
        Some(self)
        // ChunkedArray::try_from(array)
        //     .ok()
        //     .map(|_| self as &dyn EncodingCompressor)
    }

    fn compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        let compress_child_like = match like {
            None => None,
            Some(tree) => {
                if tree.children.len() != 1 {
                    vortex_bail!("chunked array compression tree should have exactly one child");
                }
                tree.children[0].clone()
            }
        };

        if let Ok(chunked_array) = ChunkedArray::try_from(array) {
            self.compress_chunked(&chunked_array, compress_child_like, ctx)
        } else {
            let (array, like) = ctx
                .compress(array, compress_child_like.as_ref())?
                .into_parts();
            Ok(CompressedArray::new(
                array,
                Some(CompressionTree::new(self, vec![like])),
            ))
        }
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([])
    }
}
