use std::io;
use std::io::Read;

use arrow::buffer::{BooleanBuffer, MutableBuffer};

use crate::array::bool::{BoolArray, BoolEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for BoolArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        ctx.write_usize(self.len())?;
        ctx.writer().write_all(self.buffer().sliced().as_slice())
    }
}

impl EncodingSerde for BoolEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.read()?)
        } else {
            None
        };
        let values_len = ctx.read_usize()?;
        let buffer_len = (values_len + 7) / 8;
        let mut buffer = Vec::<u8>::with_capacity(buffer_len);
        ctx.reader()
            .take(buffer_len as u64)
            .read_to_end(&mut buffer)?;
        Ok(BoolArray::new(
            BooleanBuffer::new(MutableBuffer::from(buffer).into(), 0, values_len),
            validity,
        )
        .boxed())
    }
}
