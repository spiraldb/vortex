use std::io;

use crate::array::varbin::{VarBinArray, VarBinEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for VarBinArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        ctx.write(self.offsets())?;
        ctx.write(self.bytes())
    }
}

impl EncodingSerde for VarBinEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.read()?)
        } else {
            None
        };
        let offsets = ctx.read()?;
        let bytes = ctx.read()?;
        Ok(VarBinArray::new(offsets, bytes, ctx.schema().clone(), validity).boxed())
    }
}
