use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use vortex::array::{
    check_validity_buffer, Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef,
};
use vortex::compress::EncodingCompression;
use vortex::dtype::DType;
use vortex::error::VortexResult;
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::scalar::{NullableScalar, Scalar};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};

mod compress;
mod serde;

#[derive(Debug, Clone)]
pub struct DeltaArray {
    encoded: ArrayRef,
    validity: Option<ArrayRef>,
    stats: Arc<RwLock<StatsSet>>,
}

impl DeltaArray {
    pub fn try_new(encoded: ArrayRef, validity: Option<ArrayRef>) -> VortexResult<Self> {
        Ok(Self {
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
            .map(|v| v.scalar_at(index).and_then(|v| v.try_into()).unwrap())
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
        self.encoded.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.encoded.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.encoded.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, _index: usize) -> VortexResult<Box<dyn Scalar>> {
        todo!()
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
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

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for DeltaArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for DeltaArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln(format!("packed: u{}", self.bit_width()))?;
        if let Some(v) = self.validity() {
            f.writeln("validity:")?;
            f.indent(|indent| indent.array(v.as_ref()))?;
        }
        f.array(self.encoded())
    }
}

impl StatsCompute for DeltaArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        // TODO(ngates): implement based on the encoded array
        StatsSet::from(HashMap::new())
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
