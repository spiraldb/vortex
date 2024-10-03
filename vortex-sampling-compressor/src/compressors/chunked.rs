use std::collections::HashSet;

use vortex::array::{Chunked, ChunkedArray};
use vortex::encoding::EncodingRef;
use vortex::{Array, ArrayDType, ArrayDef, IntoArray};
use vortex_error::{vortex_bail, VortexResult};

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct ChunkedCompressor;

impl EncodingCompressor for ChunkedCompressor {
    fn id(&self) -> &str {
        Chunked::ID.as_ref()
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
        let array = ChunkedArray::try_from(array)?;

        let mut previous = match like {
            None => None,
            Some(tree) => {
                if tree.children.len() != 1 {
                    vortex_bail!("chunked array compression tree should have exactly one child");
                }
                tree.children[0].clone()
            }
        };
        let mut target_ratio: Option<f32> = None;

        let less_chunked = array.rechunk(
            ctx.options().target_block_bytesize,
            ctx.options().target_block_size,
        )?;
        let mut compressed_chunks = Vec::with_capacity(less_chunked.nchunks());
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

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([])
    }
}
