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

use croaring::Bitmap;

use vortex::array::bool::{BoolArray, BoolEncoding};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use vortex::dtype::DType;
use vortex::dtype::Nullability::NonNullable;

use crate::boolean::{RoaringBoolArray, RoaringBoolEncoding};

impl EncodingCompression for RoaringBoolEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support bool enc arrays
        if array.encoding().id() != &BoolEncoding::ID {
            return None;
        }

        // Only support non-nullable bool arrays
        if array.dtype() != &DType::Bool(NonNullable) {
            return None;
        }

        if array.len() > u32::MAX as usize {
            return None;
        }

        Some(&(roaring_compressor as Compressor))
    }
}

fn roaring_compressor(array: &dyn Array, _like: Option<&dyn Array>, _ctx: CompressCtx) -> ArrayRef {
    roaring_encode(array.as_bool()).boxed()
}

pub fn roaring_encode(bool_array: &BoolArray) -> RoaringBoolArray {
    let mut bitmap = Bitmap::new();
    bitmap.extend(
        bool_array
            .buffer()
            .iter()
            .enumerate()
            .filter(|(_, b)| *b)
            .map(|(i, _)| i as u32),
    );
    bitmap.run_optimize();
    bitmap.shrink_to_fit();

    RoaringBoolArray::new(bitmap, bool_array.buffer().len())
}
