use std::io;

use enc::array::{Array, ArrayRef};
use enc::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{ZigZagArray, ZigZagEncoding};

impl ArraySerde for ZigZagArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write(self.encoded())
    }
}

impl EncodingSerde for ZigZagEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let encoded = ctx.read()?;
        Ok(ZigZagArray::new(encoded).boxed())
    }
}
