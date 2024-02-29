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

use arrow::buffer::BooleanBuffer;

use crate::array::bool::{BoolArray, BoolEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for BoolArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        ctx.write_buffer(self.len(), &self.buffer().sliced())
    }
}

impl EncodingSerde for BoolEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.validity().read()?)
        } else {
            None
        };

        let (logical_len, buf) = ctx.read_buffer(|len| (len + 7) / 8)?;
        Ok(BoolArray::new(BooleanBuffer::new(buf, 0, logical_len), validity).boxed())
    }
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = BoolArray::from_iter(vec![Some(false), None, Some(true), Some(false)]);
        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        assert_eq!(arr.buffer().values(), read_arr.as_bool().buffer().values());
        assert_eq!(
            arr.validity().unwrap().as_bool().buffer().values(),
            read_arr
                .as_bool()
                .validity()
                .unwrap()
                .as_bool()
                .buffer()
                .values()
        );
    }
}
