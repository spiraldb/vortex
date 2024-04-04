use vortex_error::VortexResult;

use crate::array::validity::{Validity, ValidityEncoding};
use crate::array::ArrayRef;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for Validity {
    fn write(&self, _ctx: &mut WriteCtx) -> VortexResult<()> {
        todo!()
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        // TODO: Implement this
        Ok(None)
    }
}

impl EncodingSerde for ValidityEncoding {
    fn read(&self, _ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        todo!()
    }
}
