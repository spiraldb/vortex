use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use vortex::validity::OwnedValidity;
use vortex_error::VortexResult;

use crate::{BitPackedArray, BitPackedEncoding};

impl ArraySerde for BitPackedArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write(self.encoded())?;
        ctx.write_validity(self.validity())?;
        ctx.write_optional_array(self.patches())?;
        ctx.write_usize(self.bit_width())?;
        ctx.write_usize(self.len())?;
        ctx.write_usize(self.offset())
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        let mut vec = Vec::new();
        let mut ctx = WriteCtx::new(&mut vec);
        ctx.write_usize(self.bit_width())?;
        ctx.write_usize(self.len())?;
        ctx.write_usize(self.offset())?;
        Ok(Some(vec))
    }
}

impl EncodingSerde for BitPackedEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let encoded = ctx.bytes().read()?;
        let validity = ctx.read_validity()?;
        let patches = ctx.read_optional_array()?;
        let bit_width = ctx.read_usize()?;
        let len = ctx.read_usize()?;
        let offset = ctx.read_usize()?;
        Ok(BitPackedArray::try_new_from_offset(
            encoded,
            validity,
            patches,
            bit_width,
            ctx.schema().clone(),
            len,
            offset,
        )?
        .into_array())
    }
}
