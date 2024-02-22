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
        for (i, f) in fields.iter_mut().enumerate() {
            *f = ctx.subfield(i).read()?;
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
