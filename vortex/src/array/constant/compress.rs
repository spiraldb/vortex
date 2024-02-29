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

use crate::array::constant::{ConstantArray, ConstantEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use crate::stats::Stat;

impl EncodingCompression for ConstantEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.stats().get_or_compute_or(false, &Stat::IsConstant) {
            Some(&(constant_compressor as Compressor))
        } else {
            None
        }
    }
}

fn constant_compressor(
    array: &dyn Array,
    _like: Option<&dyn Array>,
    _ctx: CompressCtx,
) -> ArrayRef {
    ConstantArray::new(array.scalar_at(0).unwrap(), array.len()).boxed()
}
