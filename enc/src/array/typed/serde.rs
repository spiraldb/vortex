use std::io;

use crate::array::typed::{TypedArray, TypedEncoding};
use crate::array::ArrayRef;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for TypedArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.dtype().write(self.untyped_array().dtype())?;
        ctx.write(self.untyped_array())
    }
}

impl EncodingSerde for TypedEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let inner_dtype = ctx.dtype().read()?;
        let mut inner_ctx = ReadCtx::new(&inner_dtype, ctx.reader());
        inner_ctx.read()
    }
}
