use std::any::Any;
use std::sync::{Arc, RwLock};

use linkme::distributed_slice;

use crate::array::composite::typed::TypedCompositeArray;
use crate::array::composite::{CompositeID, CompositeMetadata};
use crate::array::{Array, ArrayRef, Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::compress::EncodingCompression;
use crate::compute::ArrayCompute;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};

#[derive(Debug, Clone)]
pub struct CompositeArray {
    id: CompositeID,
    metadata: Arc<Vec<u8>>,
    underlying: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl CompositeArray {
    pub fn new(id: CompositeID, metadata: Arc<Vec<u8>>, underlying: ArrayRef) -> Self {
        let dtype = DType::Composite(id, underlying.dtype().is_nullable().into());
        Self {
            id,
            metadata,
            underlying,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    #[inline]
    pub fn id(&self) -> CompositeID {
        self.id
    }

    pub fn metadata<M: CompositeMetadata>(&self) -> VortexResult<M> {
        if self.id != M::ID {
            panic!("Invalid metadata type");
        }
        M::deserialize(self.metadata.as_ref())
    }

    #[inline]
    pub fn underlying(&self) -> &dyn Array {
        self.underlying.as_ref()
    }

    pub fn as_typed<M: CompositeMetadata>(&self) -> VortexResult<TypedCompositeArray<M>> {
        Ok(TypedCompositeArray::new(
            Arc::new(self.metadata::<M>()?),
            dyn_clone::clone_box(self.underlying()),
        ))
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

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        todo!()
        // Ok(Self::new(
        //     self.id().clone(),
        //     self.metadata().clone(),
        //     self.underlying.slice(start, stop)?,
        // )
        // .boxed())
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
        None
        // Some(self)
    }
}

impl StatsCompute for CompositeArray {}

impl ArrayCompute for CompositeArray {}

impl<'arr> AsRef<(dyn Array + 'arr)> for CompositeArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

// FIXME(ngates): macro this
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
        None
        // Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        None
        // Some(self)
    }
}

impl ArrayDisplay for CompositeArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("id", self.id())?;
        // TODO(ngates): downcast?
        // f.property("metadata", self.metadata())?;
        f.child("underlying", self.underlying())
    }
}
//
// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::composite_dtypes::{localtime, TimeUnit, TimeUnitSerializer};
//     use crate::compute::scalar_at::scalar_at;
//     use crate::dtype::{IntWidth, Nullability};
//     use crate::scalar::{CompositeScalar, PScalar, PrimitiveScalar};
//     //
//     // #[test]
//     // pub fn scalar() {
//     //     let dtype = localtime(TimeUnit::Us, IntWidth::_64, Nullability::NonNullable);
//     //     let arr = CompositeArray::new(
//     //         Arc::new("localtime".into()),
//     //         TimeUnitSerializer::serialize(TimeUnit::Us),
//     //         vec![64_799_000_000_i64, 43_000_000_000].into(),
//     //     );
//     //     assert_eq!(
//     //         scalar_at(arr.as_ref(), 0).unwrap(),
//     //         CompositeScalar::new(
//     //             dtype.clone(),
//     //             Box::new(PrimitiveScalar::some(PScalar::I64(64_799_000_000)).into()),
//     //         )
//     //         .into()
//     //     );
//     //     assert_eq!(
//     //         scalar_at(arr.as_ref(), 1).unwrap(),
//     //         CompositeScalar::new(
//     //             dtype.clone(),
//     //             Box::new(PrimitiveScalar::some(PScalar::I64(43_000_000_000)).into()),
//     //         )
//     //         .into()
//     //     );
//     // }
// }
