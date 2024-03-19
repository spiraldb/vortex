use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::{Arc, RwLock};

use crate::array::composite::untyped::CompositeEncoding;
use crate::array::composite::CompositeID;
use crate::array::{Array, ArrayRef, EncodingRef};
use crate::compute::ArrayCompute;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::ArraySerde;
use crate::stats::{Stats, StatsCompute, StatsSet};

pub trait CompositeArrayPlugin: 'static {}

pub trait CompositeMetadata: Debug + Display + Clone + Send + Sync + 'static {
    const ID: CompositeID;

    fn deserialize(metadata: &[u8]) -> VortexResult<Self>;
}

#[derive(Debug, Clone)]
pub struct TypedCompositeArray<M: CompositeMetadata> {
    metadata: Arc<M>,
    underlying: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl<M: CompositeMetadata> TypedCompositeArray<M> {
    pub fn new(metadata: Arc<M>, underlying: ArrayRef) -> Self {
        let dtype = DType::Composite(M::ID, underlying.dtype().is_nullable().into());
        Self {
            metadata,
            underlying,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    #[inline]
    pub fn id(&self) -> CompositeID {
        M::ID
    }

    #[inline]
    pub fn metadata(&self) -> &Arc<M> {
        &self.metadata
    }

    #[inline]
    pub fn underlying(&self) -> &dyn Array {
        self.underlying.as_ref()
    }
}

impl<M: CompositeMetadata> Array for TypedCompositeArray<M> {
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
    }
}

impl<M: CompositeMetadata> StatsCompute for TypedCompositeArray<M> {}

impl<M: CompositeMetadata> ArrayCompute for TypedCompositeArray<M> {}

impl<'arr, M: CompositeMetadata> AsRef<(dyn Array + 'arr)> for TypedCompositeArray<M> {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

macro_rules! composite_impl {
    ($id:expr, $T:ty) => {
        use crate::array::{Encoding, EncodingId, EncodingRef, ENCODINGS};
        use linkme::distributed_slice;
        use paste::paste;

        paste! {
            #[derive(Debug)]
            pub struct [<$T Encoding>];

            impl [<$T Encoding>] {
                pub const ID: EncodingId = EncodingId::new($id);
            }

            impl Encoding for [<$T Encoding>] {
                fn id(&self) -> &EncodingId {
                    &Self::ID
                }
            }

            #[distributed_slice(ENCODINGS)]
            static ENCODINGS_COMPOSITE: EncodingRef = &[<$T Encoding>];
        }
    };
}

pub(crate) use composite_impl;

impl<M: CompositeMetadata> ArrayDisplay for TypedCompositeArray<M> {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("id", self.id())?;
        f.property("metadata", self.metadata())?;
        f.child("underlying", self.underlying())
    }
}
