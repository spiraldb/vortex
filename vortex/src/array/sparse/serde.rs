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
use std::io::ErrorKind;

use crate::array::sparse::{SparseArray, SparseEncoding};
use crate::array::{Array, ArrayRef};
use crate::dtype::DType;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for SparseArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_usize(self.len())?;
        // TODO(robert): Rewrite indices and don't store offset
        ctx.write_usize(self.indices_offset())?;
        ctx.write(self.indices())?;
        ctx.write(self.values())
    }
}

impl EncodingSerde for SparseEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let len = ctx.read_usize()?;
        let offset = ctx.read_usize()?;
        let indices = ctx.with_schema(&DType::IDX).read()?;
        let values = ctx.read()?;
        Ok(SparseArray::new_with_offset(indices, values, len, offset)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?
            .boxed())
    }
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::sparse::SparseArray;
    use crate::array::Array;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = SparseArray::new(
            PrimitiveArray::from_vec(vec![7u64, 37, 71, 97]).boxed(),
            PrimitiveArray::from_iter(vec![Some(0), None, Some(2), Some(42)]).boxed(),
            100,
        );

        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        assert_eq!(
            arr.indices().as_primitive().buffer().typed_data::<u8>(),
            read_arr
                .as_sparse()
                .indices()
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );

        assert_eq!(
            arr.values().as_primitive().buffer().typed_data::<i32>(),
            read_arr
                .as_sparse()
                .values()
                .as_primitive()
                .buffer()
                .typed_data::<i32>()
        );
    }
}
