use std::io;
use std::io::{ErrorKind, Read};

use arrow::buffer::MutableBuffer;

use crate::array::primitive::{PrimitiveArray, PrimitiveEncoding};
use crate::array::{Array, ArrayRef};
use crate::ptype::PType;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for PrimitiveArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        ctx.write_usize(self.len())?;
        ctx.writer().write_all(self.buffer().as_slice())
    }
}

impl EncodingSerde for PrimitiveEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.read()?)
        } else {
            None
        };
        let values_len = ctx.read_usize()?;
        let mut buffer = Vec::<u8>::with_capacity(values_len);
        ctx.reader()
            .take(values_len as u64)
            .read_to_end(&mut buffer)?;
        let ptype =
            PType::try_from(ctx.schema()).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        Ok(PrimitiveArray::new(ptype, MutableBuffer::from(buffer).into(), validity).boxed())
    }
}
