use linkme::distributed_slice;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use crate::stats::Stats;
use crate::validity::owned::Validity;
use crate::validity::ValidityView;
use crate::{impl_array, ArrayWalker};

pub trait ArrayValidity {
    fn logical_validity(&self) -> Validity;

    fn is_valid(&self, index: usize) -> bool;
}

pub trait OwnedValidity {
    fn validity(&self) -> Option<ValidityView>;
}

impl<T: Array + OwnedValidity> ArrayValidity for T {
    fn logical_validity(&self) -> Validity {
        self.validity()
            .and_then(|v| v.logical_validity())
            .unwrap_or_else(|| Validity::Valid(self.len()))
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .map_or(true, |v| ValidityView::is_valid(&v, index))
    }
}

impl Array for Validity {
    impl_array!();

    fn len(&self) -> usize {
        match self {
            Validity::Valid(len) | Validity::Invalid(len) => *len,
            Validity::Array(a) => a.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Validity::Valid(len) | Validity::Invalid(len) => *len == 0,
            Validity::Array(a) => a.is_empty(),
        }
    }

    fn dtype(&self) -> &DType {
        &Validity::DTYPE
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(self.slice(start, stop).into_array())
    }

    fn encoding(&self) -> EncodingRef {
        &ValidityEncoding
    }

    fn nbytes(&self) -> usize {
        match self {
            Validity::Valid(_) | Validity::Invalid(_) => 8,
            Validity::Array(a) => a.nbytes(),
        }
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        Ok(())
    }
}

impl ArrayValidity for Validity {
    fn logical_validity(&self) -> Validity {
        // Validity is a non-nullable boolean array.
        Validity::Valid(self.len())
    }

    fn is_valid(&self, _index: usize) -> bool {
        true
    }
}

impl ArrayDisplay for Validity {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        match self {
            Validity::Valid(_) => fmt.property("all", "valid"),
            Validity::Invalid(_) => fmt.property("all", "invalid"),
            Validity::Array(a) => fmt.child("validity", a),
        }
    }
}

impl ArrayCompute for Validity {}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_VALIDITY: EncodingRef = &ValidityEncoding;

#[derive(Debug)]
struct ValidityEncoding;

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
