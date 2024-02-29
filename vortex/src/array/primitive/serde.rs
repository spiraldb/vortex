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

use crate::array::primitive::{PrimitiveArray, PrimitiveEncoding};
use crate::array::{Array, ArrayRef};
use crate::ptype::PType;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for PrimitiveArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        ctx.write_buffer(self.len(), self.buffer())
    }
}

impl EncodingSerde for PrimitiveEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.validity().read()?)
        } else {
            None
        };

        let ptype =
            PType::try_from(ctx.schema()).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        let (_, buf) = ctx.read_buffer(|len| len * ptype.byte_width())?;
        Ok(PrimitiveArray::new(ptype, buf, validity).boxed())
    }
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = PrimitiveArray::from_iter(vec![Some(0), None, Some(2), Some(42)]);
        let read_arr = roundtrip_array(arr.as_ref()).unwrap();
        assert_eq!(
            arr.buffer().typed_data::<i32>(),
            read_arr.as_primitive().buffer().typed_data::<i32>()
        );

        assert_eq!(
            arr.validity().unwrap().as_bool().buffer().values(),
            read_arr
                .as_primitive()
                .validity()
                .unwrap()
                .as_bool()
                .buffer()
                .values()
        );
    }
}
