use std::any::Any;
use std::sync::{Arc, RwLock};

use vortex::array::{Array, ArrayRef, Encoding, EncodingId, EncodingRef};
use vortex::compress::EncodingCompression;
use vortex::compute::scalar_at::scalar_at;
use vortex::compute::ArrayCompute;
use vortex::error::VortexResult;
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};
use vortex_schema::DType;

mod compress;
mod serde;

#[derive(Debug, Clone)]
pub struct DeltaArray {
    len: usize,
    encoded: ArrayRef,
    validity: Option<ArrayRef>,
    stats: Arc<RwLock<StatsSet>>,
}

impl DeltaArray {
    pub fn try_new(
        len: usize,
        encoded: ArrayRef,
        validity: Option<ArrayRef>,
    ) -> VortexResult<Self> {
        Ok(Self {
            len,
            encoded,
            validity,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn encoded(&self) -> &dyn Array {
        self.encoded.as_ref()
    }

    #[inline]
    pub fn validity(&self) -> Option<&dyn Array> {
        self.validity.as_deref()
    }

    pub fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .map(|v| scalar_at(v, index).and_then(|v| v.try_into()).unwrap())
            .unwrap_or(true)
    }
}

impl Array for DeltaArray {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.encoded.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.encoded.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        unimplemented!("DeltaArray::slice")
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &DeltaEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.encoded().nbytes() + self.validity().map(|v| v.nbytes()).unwrap_or(0)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl ArrayCompute for DeltaArray {}

impl<'arr> AsRef<(dyn Array + 'arr)> for DeltaArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for DeltaArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("deltas", self.encoded())?;
        f.maybe_child("validity", self.validity())
    }
}

impl StatsCompute for DeltaArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::default())
    }
}

#[derive(Debug)]
pub struct DeltaEncoding;

impl DeltaEncoding {
    pub const ID: EncodingId = EncodingId::new("fastlanes.delta");
}

impl Encoding for DeltaEncoding {
    fn id(&self) -> &EncodingId {
        &Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
