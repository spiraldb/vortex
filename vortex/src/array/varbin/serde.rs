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

use std::io;

use crate::array::varbin::{VarBinArray, VarBinEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for VarBinArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        ctx.dtype(self.offsets().dtype())?;
        ctx.write(self.offsets())?;
        ctx.write(self.bytes())
    }
}

impl EncodingSerde for VarBinEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.validity().read()?)
        } else {
            None
        };
        // TODO(robert): Stop writing this
        let offsets_dtype = ctx.dtype()?;
        let offsets = ctx.with_schema(&offsets_dtype).read()?;
        let bytes = ctx.bytes().read()?;
        Ok(VarBinArray::new(offsets, bytes, ctx.schema().clone(), validity).boxed())
    }
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::varbin::VarBinArray;
    use crate::dtype::{DType, Nullability};
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = VarBinArray::from_vec(
            vec!["a", "def", "hello", "this", "is", "a", "test"],
            DType::Utf8(Nullability::NonNullable),
        );

        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        assert_eq!(
            arr.offsets().as_primitive().buffer().typed_data::<u32>(),
            read_arr
                .as_varbin()
                .offsets()
                .as_primitive()
                .buffer()
                .typed_data::<u32>()
        );

        assert_eq!(
            arr.bytes().as_primitive().buffer().typed_data::<u8>(),
            read_arr
                .as_varbin()
                .bytes()
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );
    }
}
