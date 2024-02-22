use std::io;

use crate::array::constant::{ConstantArray, ConstantEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for ConstantArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_usize(self.len())?;
        ctx.scalar().write(self.scalar())
    }
}

impl EncodingSerde for ConstantEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let len = ctx.read_usize()?;
        let scalar = ctx.scalar().read()?;
        Ok(ConstantArray::new(scalar, len).boxed())
    }
}
