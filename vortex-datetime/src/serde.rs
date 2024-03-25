use crate::{DateTimeArray, DateTimeEncoding};
use vortex::array::{Array, ArrayRef};
use vortex::error::VortexResult;
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for DateTimeArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.dtype(self.days().dtype())?;
        ctx.write(self.days())?;
        ctx.dtype(self.seconds().dtype())?;
        ctx.write(self.seconds())?;
        ctx.dtype(self.subsecond().dtype())?;
        ctx.write(self.subsecond())?;
        ctx.write_optional_array(self.validity())
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
            ctx.validity().read_optional_array()?,
            ctx.schema().clone(),
        )
        .into_array())
    }
}
