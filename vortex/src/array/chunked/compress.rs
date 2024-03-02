// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use rayon::prelude::*;

use crate::array::chunked::{ChunkedArray, ChunkedEncoding, CHUNKED_ENCODING};
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};

impl EncodingCompression for ChunkedEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() == &CHUNKED_ENCODING {
            Some(&(chunked_compressor as Compressor))
        } else {
            None
        }
    }
}

fn chunked_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let chunked_array = array.as_chunked();
    let chunked_like = like.map(|like_array| like_array.as_chunked());

    let compressed_chunks = chunked_like
        .map(|c_like| {
            chunked_array
                .chunks()
                .par_iter()
                .zip_eq(c_like.chunks())
                .map(|(chunk, chunk_like)| ctx.compress(chunk.as_ref(), Some(chunk_like.as_ref())))
                .collect()
        })
        .unwrap_or_else(|| {
            chunked_array
                .chunks()
                .par_iter()
                .map(|chunk| ctx.compress(chunk.as_ref(), None))
                .collect()
        });

    ChunkedArray::new(compressed_chunks, array.dtype().clone()).boxed()
}
