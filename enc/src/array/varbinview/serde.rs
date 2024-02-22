use crate::array::varbinview::{VarBinViewArray, VarBinViewEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use std::io;

impl ArraySerde for VarBinViewArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }
        ctx.write(self.views())?;
        ctx.write_usize(self.data().len())?;
        for d in self.data() {
            ctx.write(d.as_ref())?;
        }
        Ok(())
    }
}

impl EncodingSerde for VarBinViewEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.read()?)
        } else {
            None
        };
        let views = ctx.read()?;
        let num_data = ctx.read_usize()?;
        let mut data_bufs = Vec::<ArrayRef>::with_capacity(num_data);
        for buf in data_bufs.iter_mut() {
            *buf = ctx.read()?;
        }
        Ok(VarBinViewArray::new(views, data_bufs, ctx.schema().clone(), validity).boxed())
    }
}
