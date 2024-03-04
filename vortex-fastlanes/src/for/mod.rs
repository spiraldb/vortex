use std::any::Any;
use std::sync::{Arc, RwLock};

use vortex::array::{Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};
use vortex::compress::EncodingCompression;
use vortex::compute::ArrayCompute;
use vortex::dtype::DType;
use vortex::error::VortexResult;
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::scalar::Scalar;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};

mod compress;
mod serde;

#[derive(Debug, Clone)]
pub struct FoRArray {
    child: ArrayRef,
    reference: Box<dyn Scalar>,
    stats: Arc<RwLock<StatsSet>>,
}

impl FoRArray {
    pub fn try_new(child: ArrayRef, reference: Box<dyn Scalar>) -> VortexResult<Self> {
        // TODO(ngates): check the dtype of reference == child.dtype()
        Ok(Self {
            child,
            reference,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn child(&self) -> &dyn Array {
        self.child.as_ref()
    }

    #[inline]
    pub fn reference(&self) -> &dyn Scalar {
        self.reference.as_ref()
    }
}

impl Array for FoRArray {
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
        self.child.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.child.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.child.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(Self {
            child: self.child.slice(start, stop)?,
            reference: self.reference.clone(),
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &FoREncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.child.nbytes() + self.reference.nbytes()
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl ArrayCompute for FoRArray {}

impl<'arr> AsRef<(dyn Array + 'arr)> for FoRArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for FoRArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln(format!("reference: {}", self.reference))?;
        f.indent(|indent| indent.array(self.child()))
    }
}

impl StatsCompute for FoRArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::default())
    }
}

#[derive(Debug)]
pub struct FoREncoding;

impl FoREncoding {
    pub const ID: EncodingId = EncodingId::new("fastlanes.for");
}

impl Encoding for FoREncoding {
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
