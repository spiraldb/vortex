use crate::{DictArray, DictEncoding};
use enc::array::{Array, ArrayRef};
use enc::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use std::io;

impl ArraySerde for DictArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write(self.dict())?;
        ctx.write(self.codes())
    }
}

impl EncodingSerde for DictEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let dict = ctx.read()?;
        let codes = ctx.read()?;
        Ok(DictArray::new(codes, dict).boxed())
    }
}
