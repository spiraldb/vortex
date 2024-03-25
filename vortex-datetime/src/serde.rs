use crate::{DateTimeArray, DateTimeEncoding};
use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use vortex::validity::ArrayValidity;
use vortex_error::VortexResult;

impl ArraySerde for DateTimeArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.dtype(self.days().dtype())?;
        ctx.write(self.days())?;
        ctx.dtype(self.seconds().dtype())?;
        ctx.write(self.seconds())?;
        ctx.dtype(self.subsecond().dtype())?;
        ctx.write(self.subsecond())?;
        ctx.write_validity(self.validity())
    }
}

impl EncodingSerde for DateTimeEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let days_dtype = ctx.dtype()?;
        let days = ctx.with_schema(&days_dtype).read()?;
        let seconds_dtype = ctx.dtype()?;
        let seconds = ctx.with_schema(&seconds_dtype).read()?;
        let subseconds_dtype = ctx.dtype()?;
        let subsecs = ctx.with_schema(&subseconds_dtype).read()?;
        Ok(DateTimeArray::new(
            days,
            seconds,
            subsecs,
            ctx.read_validity()?,
            ctx.schema().clone(),
        )
        .into_array())
    }
}
