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

use crate::array::struct_::{StructArray, StructEncoding};
use crate::array::{Array, ArrayRef};
use crate::dtype::DType;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for StructArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_usize(self.fields().len())?;
        for f in self.fields() {
            ctx.write(f.as_ref())?;
        }
        Ok(())
    }
}

impl EncodingSerde for StructEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let num_fields = ctx.read_usize()?;
        let mut fields = Vec::<ArrayRef>::with_capacity(num_fields);
        // TODO(robert): use read_vectored
        for i in 0..num_fields {
            fields.push(ctx.subfield(i).read()?);
        }
        let DType::Struct(ns, _) = ctx.schema() else {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "invalid schema type",
            ));
        };
        Ok(StructArray::new(ns.clone(), fields).boxed())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::struct_::StructArray;
    use crate::array::Array;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = StructArray::new(
            vec![
                Arc::new("primes".to_string()),
                Arc::new("nullable".to_string()),
            ],
            vec![
                PrimitiveArray::from_vec(vec![7u8, 37, 71, 97]).boxed(),
                PrimitiveArray::from_iter(vec![Some(0), None, Some(2), Some(42)]).boxed(),
            ],
        );

        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        assert_eq!(
            arr.fields()[0].as_primitive().buffer().typed_data::<u8>(),
            read_arr.as_struct().fields()[0]
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );

        assert_eq!(
            arr.fields()[1].as_primitive().buffer().typed_data::<i32>(),
            read_arr.as_struct().fields()[1]
                .as_primitive()
                .buffer()
                .typed_data::<i32>()
        );
    }
}
