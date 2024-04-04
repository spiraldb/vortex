use linkme::distributed_slice;
use vortex_error::VortexResult;

use crate::array::ArrayRef;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::serde::{EncodingSerde, ReadCtx};

#[distributed_slice(ENCODINGS)]
static ENCODINGS_VALIDITY: EncodingRef = &ValidityEncoding;

#[derive(Debug)]
pub struct ValidityEncoding;

impl ValidityEncoding {
    const ID: EncodingId = EncodingId::new("vortex.validity");
}

impl Encoding for ValidityEncoding {
    fn id(&self) -> EncodingId {
        ValidityEncoding::ID
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

impl EncodingSerde for ValidityEncoding {
    fn read(&self, _ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        todo!()
    }
}
