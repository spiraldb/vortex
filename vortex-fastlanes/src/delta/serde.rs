use vortex::array::{Array, ArrayRef};
use vortex::error::VortexResult;
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{DeltaArray, DeltaEncoding};

impl ArraySerde for DeltaArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_usize(self.len())?;
        ctx.write(self.encoded())?;
        ctx.write_optional_array(self.validity())
    }
}

impl EncodingSerde for DeltaEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let len = ctx.read_usize()?;
        let encoded = ctx.read()?;
        let validity = ctx.read_optional_array()?;
        Ok(DeltaArray::try_new(len, encoded, validity).unwrap().into_array())
    }
}
