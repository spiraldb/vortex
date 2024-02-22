use std::io;
use std::io::ErrorKind;

use crate::array::sparse::{SparseArray, SparseEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for SparseArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_usize(self.len())?;
        ctx.write_usize(self.indices_offset())?;
        ctx.write(self.indices())?;
        ctx.write(self.values())
    }
}

impl EncodingSerde for SparseEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let len = ctx.read_usize()?;
        let offset = ctx.read_usize()?;
        let indices = ctx.read()?;
        let values = ctx.read()?;
        Ok(SparseArray::new_with_offset(indices, values, len, offset)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?
            .boxed())
    }
}
