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

use crate::array::primitive::PrimitiveEncoding;
use crate::array::{Array, ArrayRef};
use crate::compress::{
    sampled_compression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};

impl EncodingCompression for PrimitiveEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() == &Self::ID {
            Some(&(primitive_compressor as Compressor))
        } else {
            None
        }
    }
}

fn primitive_compressor(
    array: &dyn Array,
    _like: Option<&dyn Array>,
    ctx: CompressCtx,
) -> ArrayRef {
    sampled_compression(array, ctx)
}

#[cfg(test)]
mod test {
    use crate::array::constant::ConstantEncoding;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Encoding;
    use crate::compress::CompressCtx;

    #[test]
    pub fn compress_constant() {
        let arr = PrimitiveArray::from_vec(vec![1, 1, 1, 1]);
        let res = CompressCtx::default().compress(arr.as_ref(), None);
        assert_eq!(res.encoding().id(), ConstantEncoding.id());
        assert_eq!(res.scalar_at(3).unwrap().try_into(), Ok(1));
    }
}
