use std::any::Any;
use std::sync::{Arc, RwLock};

use linkme::distributed_slice;

use crate::array::{Array, ArrayRef, Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::compress::EncodingCompression;
use crate::dtype::{DType, Metadata};
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};

mod as_arrow;
mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct CompositeArray {
    underlying: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl CompositeArray {
    pub fn new(id: Arc<String>, metadata: Metadata, underlying: ArrayRef) -> Self {
        let dtype = DType::Composite(id, Box::new(underlying.dtype().clone()), metadata);
        Self {
            underlying,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    pub fn id(&self) -> Arc<String> {
        let DType::Composite(id, _, _) = &self.dtype else {
            unreachable!()
        };
        id.clone()
    }

    pub fn metadata(&self) -> &Metadata {
        let DType::Composite(_, _, metadata) = &self.dtype else {
            unreachable!()
        };
        metadata
    }

    #[inline]
    pub fn underlying(&self) -> &dyn Array {
        self.underlying.as_ref()
    }
}

impl Array for CompositeArray {
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
        self.underlying.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.underlying.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(Self::new(
            self.id().clone(),
            self.metadata().clone(),
            self.underlying.slice(start, stop)?,
        )
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &CompositeEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.underlying.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl StatsCompute for CompositeArray {}

impl<'arr> AsRef<(dyn Array + 'arr)> for CompositeArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

#[derive(Debug)]
pub struct CompositeEncoding;

impl CompositeEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.composite");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_COMPOSITE: EncodingRef = &CompositeEncoding;

impl Encoding for CompositeEncoding {
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

impl ArrayDisplay for CompositeArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("composite", self.underlying())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::composite_dtypes::{localtime, TimeUnit, TimeUnitSerializer};
    use crate::compute::scalar_at::scalar_at;
    use crate::dtype::{IntWidth, Nullability};
    use crate::scalar::{CompositeScalar, PScalar, PrimitiveScalar};

    #[test]
    pub fn scalar() {
        let dtype = localtime(TimeUnit::Us, IntWidth::_64, Nullability::NonNullable);
        let arr = CompositeArray::new(
            Arc::new("localtime".into()),
            TimeUnitSerializer::serialize(TimeUnit::Us),
            vec![64_799_000_000_i64, 43_000_000_000].into(),
        );
        assert_eq!(
            scalar_at(arr.as_ref(), 0).unwrap(),
            CompositeScalar::new(
                dtype.clone(),
                Box::new(PrimitiveScalar::some(PScalar::I64(64_799_000_000)).into()),
            )
            .into()
        );
        assert_eq!(
            scalar_at(arr.as_ref(), 1).unwrap(),
            CompositeScalar::new(
                dtype.clone(),
                Box::new(PrimitiveScalar::some(PScalar::I64(43_000_000_000)).into()),
            )
            .into()
        );
    }
}
