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

use crate::{FoRArray, FoREncoding};

impl ArraySerde for FoRArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.scalar(self.reference())?;
        ctx.write(self.child())
    }
}

impl EncodingSerde for FoREncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let reference = ctx.scalar()?;
        let child = ctx.read()?;
        Ok(FoRArray::try_new(child, reference).unwrap().boxed())
    }
}

#[cfg(test)]
mod test {
    use crate::FoRArray;
    use std::io;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::{Array, ArrayRef};
    use vortex::scalar::Scalar;
    use vortex::serde::{ReadCtx, WriteCtx};

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
        let arr = FoRArray::try_new(
            PrimitiveArray::from_vec(vec![-7i64, -13, 17, 23]).boxed(),
            <i64 as Into<Box<dyn Scalar>>>::into(-7i64),
        )
        .unwrap();
        roundtrip_array(arr.as_ref()).unwrap();
    }
}
