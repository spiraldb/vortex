use std::io;

use crate::array::chunked::{ChunkedArray, ChunkedEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for ChunkedArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_usize(self.chunks().len())?;
        for c in self.chunks() {
            ctx.write(c.as_ref())?;
        }
        Ok(())
    }
}

impl EncodingSerde for ChunkedEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let chunk_len = ctx.read_usize()?;
        let mut chunks = Vec::<ArrayRef>::with_capacity(chunk_len);
        for c in chunks.iter_mut() {
            *c = ctx.read()?;
        }
        Ok(ChunkedArray::new(chunks, ctx.schema().clone()).boxed())
    }
}
