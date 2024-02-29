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

use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{DictArray, DictEncoding};

impl ArraySerde for DictArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write(self.dict())?;
        // TODO(robert): Stop writing this
        ctx.dtype(self.codes().dtype())?;
        ctx.write(self.codes())
    }
}

impl EncodingSerde for DictEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let dict = ctx.read()?;
        let codes_dtype = ctx.dtype()?;
        let codes = ctx.with_schema(&codes_dtype).read()?;
        Ok(DictArray::new(codes, dict).boxed())
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::{Array, ArrayRef};
    use vortex::serde::{ReadCtx, WriteCtx};

    use crate::downcast::DowncastDict;
    use crate::DictArray;

    fn roundtrip_array(array: &dyn Array) -> io::Result<ArrayRef> {
        let mut buf = Vec::<u8>::new();
        let mut write_ctx = WriteCtx::new(&mut buf);
        write_ctx.write(array)?;
        let mut read = buf.as_slice();
        let mut read_ctx = ReadCtx::new(array.dtype(), &mut read);
        read_ctx.read()
    }

    #[test]
    fn roundtrip() {
        let arr = DictArray::new(
            PrimitiveArray::from_vec(vec![0u8, 0, 1, 2, 3]).boxed(),
            PrimitiveArray::from_vec(vec![-7i64, -13, 17, 23]).boxed(),
        );
        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        assert_eq!(
            arr.codes().as_primitive().buffer().typed_data::<u8>(),
            read_arr
                .as_dict()
                .codes()
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );

        assert_eq!(
            arr.dict().as_primitive().buffer().typed_data::<i64>(),
            read_arr
                .as_dict()
                .dict()
                .as_primitive()
                .buffer()
                .typed_data::<i64>()
        );
    }
}
