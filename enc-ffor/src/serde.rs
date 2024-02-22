use std::io;

use enc::array::{Array, ArrayRef};
use enc::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{FFORArray, FFoREncoding};

impl ArraySerde for FFORArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_usize(self.len())?;
        ctx.writer().write_all(&[self.num_bits()])?;
        ctx.scalar().write(self.min_val())?;
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        if let Some(p) = self.patches() {
            ctx.writer().write_all(&[0x01])?;
            ctx.write(p.as_ref())?;
        } else {
            ctx.writer().write_all(&[0x00])?;
        }
        ctx.write(self.encoded())
    }
}

impl EncodingSerde for FFoREncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let len = ctx.read_usize()?;
        let num_bits = ctx.read_nbytes::<1>()?[0];
        let min_val = ctx.scalar().read()?;
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.read()?)
        } else {
            None
        };
        let patches_tag = ctx.read_nbytes::<1>()?[0];
        let patches = if patches_tag == 0x01 {
            Some(ctx.read()?)
        } else {
            None
        };
        let encoded = ctx.read()?;
        Ok(FFORArray::new(encoded, validity, patches, min_val, num_bits, len).boxed())
    }
}
