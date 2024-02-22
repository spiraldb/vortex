use crate::{REEArray, REEEncoding};
use enc::array::{Array, ArrayRef};
use enc::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use std::io;

impl ArraySerde for REEArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_usize(self.len())?;
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        ctx.write(self.ends())?;
        ctx.write(self.values())
    }
}

impl EncodingSerde for REEEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let len = ctx.read_usize()?;
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.read()?)
        } else {
            None
        };
        let ends = ctx.read()?;
        let values = ctx.read()?;
        Ok(REEArray::new(ends, values, validity, len).boxed())
    }
}
