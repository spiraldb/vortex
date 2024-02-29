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

use crate::array::bool::{BoolEncoding, BOOL_ENCODING};
use crate::array::{Array, ArrayRef};
use crate::compress::{
    sampled_compression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};

impl EncodingCompression for BoolEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() == &BOOL_ENCODING {
            Some(&(bool_compressor as Compressor))
        } else {
            None
        }
    }
}

fn bool_compressor(array: &dyn Array, _like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    sampled_compression(array, ctx)
}
