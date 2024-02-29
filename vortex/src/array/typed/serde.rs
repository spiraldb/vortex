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

use crate::array::typed::{TypedArray, TypedEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for TypedArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.dtype(self.untyped_array().dtype())?;
        ctx.write(self.untyped_array())
    }
}

impl EncodingSerde for TypedEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let inner_dtype = ctx.dtype()?;
        Ok(TypedArray::new(ctx.with_schema(&inner_dtype).read()?, ctx.schema().clone()).boxed())
    }
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::typed::TypedArray;
    use crate::array::Array;
    use crate::dtype::{DType, IntWidth, Nullability, Signedness};
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = TypedArray::new(
            PrimitiveArray::from_vec(vec![7u8, 37, 71, 97]).boxed(),
            DType::Int(IntWidth::_64, Signedness::Signed, Nullability::NonNullable),
        );

        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        assert_eq!(
            arr.untyped_array()
                .as_primitive()
                .buffer()
                .typed_data::<u8>(),
            read_arr
                .as_typed()
                .untyped_array()
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );

        assert_eq!(arr.dtype(), read_arr.dtype());
    }
}
