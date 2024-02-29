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

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::struct_::{StructArray, StructEncoding, STRUCT_ENCODING};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;

impl EncodingCompression for StructEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() == &STRUCT_ENCODING {
            Some(&(struct_compressor as Compressor))
        } else {
            None
        }
    }
}

fn struct_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let struct_array = array.as_struct();
    let struct_like = like.map(|like_array| like_array.as_struct());

    let fields = struct_like
        .map(|s_like| {
            struct_array
                .fields()
                .par_iter()
                .zip_eq(s_like.fields())
                .map(|(field, field_like)| ctx.compress(field.as_ref(), Some(field_like.as_ref())))
                .collect()
        })
        .unwrap_or_else(|| {
            struct_array
                .fields()
                .par_iter()
                .map(|field| ctx.compress(field.as_ref(), None))
                .collect()
        });

    StructArray::new(struct_array.names().clone(), fields).boxed()
}
