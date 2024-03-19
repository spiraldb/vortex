use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use crate::array::composite::array::CompositeArray;
use crate::array::composite::{CompositeEncoding, CompositeID, CompositeMetadata};
use crate::array::{Array, ArrayRef, EncodingRef};
use crate::compute::ArrayCompute;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, BytesSerde, WriteCtx};
use crate::stats::{Stats, StatsCompute, StatsSet};

pub trait CompositeExtension: Debug + Send + Sync + 'static {
    fn id(&self) -> CompositeID;

    fn as_typed_array(&self, array: &CompositeArray) -> ArrayRef;
}

pub type CompositeExtensionRef = &'static dyn CompositeExtension;

#[derive(Debug, Clone)]
pub struct TypedCompositeArray<M: CompositeMetadata> {
    metadata: M,
    underlying: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl<M: CompositeMetadata> TypedCompositeArray<M> {
    pub fn new(metadata: M, underlying: ArrayRef) -> Self {
        let dtype = DType::Composite(metadata.id(), underlying.dtype().is_nullable().into());
        Self {
            metadata,
            underlying,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    #[inline]
    pub fn id(&self) -> CompositeID {
        self.metadata().id()
    }

    #[inline]
    pub fn metadata(&self) -> &M {
        &self.metadata
    }

    #[inline]
    pub fn underlying(&self) -> &dyn Array {
        self.underlying.as_ref()
    }

    pub fn as_untyped(&self) -> CompositeArray {
        CompositeArray::new(
            self.id(),
            Arc::new(self.metadata().serialize()),
            dyn_clone::clone_box(self.underlying()),
        )
    }
}

impl<M: CompositeMetadata> Array for TypedCompositeArray<M>
where
    TypedCompositeArray<M>: ArrayCompute,
{
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
            self.metadata().clone(),
            self.underlying().slice(start, stop)?,
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

impl<'arr, M: CompositeMetadata> AsRef<(dyn Array + 'arr)> for TypedCompositeArray<M>
where
    TypedCompositeArray<M>: ArrayCompute,
    Arc<M>: BytesSerde,
{
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl<M: CompositeMetadata> ArrayDisplay for TypedCompositeArray<M> {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("id", self.id())?;
        f.property("metadata", self.metadata())?;
        f.child("underlying", self.underlying())
    }
}

// TODO(ngates): stats compute should run stats over the underlying, then cast into the composite
//  dtype?
impl<M: CompositeMetadata> StatsCompute for TypedCompositeArray<M> {}

impl<M: CompositeMetadata> ArraySerde for TypedCompositeArray<M> {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        self.as_untyped().serde().unwrap().write(ctx)
    }
}

impl<M: CompositeMetadata> FlattenFn for TypedCompositeArray<M> {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Composite(self.as_untyped()))
    }
}

macro_rules! composite_impl {
    ($id:expr, $T:ty) => {
        use crate::array::composite::array::{CompositeArray, CompositeMetadata};
        use crate::array::composite::typed::CompositeExtension;
        use crate::array::composite::COMPOSITE_EXTENSIONS;
        use crate::array::{Array, ArrayRef};
        use crate::dtype::{DType, Nullability};
        use linkme::distributed_slice;
        use paste::paste;

        paste! {
            #[derive(Debug)]
            pub struct [<$T Extension>];

            impl [<$T Extension>] {
                pub const ID: CompositeID = CompositeID($id);

                pub fn dtype(nullability: Nullability) -> DType {
                    DType::Composite(Self::ID, nullability)
                }
            }

            impl CompositeExtension for [<$T Extension>] {
                fn id(&self) -> CompositeID {
                    Self::ID
                }

                fn as_typed_array(&self, array: &CompositeArray) -> ArrayRef {
                    if array.id() != Self::ID {
                        panic!("Incorrect CompositeID");
                    }
                    array.as_typed::<$T>().boxed()
                }
            }

            impl CompositeMetadata for $T {
                fn id(&self) -> CompositeID {
                    [<$T Extension>]::ID
                }
            }

            #[distributed_slice(COMPOSITE_EXTENSIONS)]
            static ENCODINGS_COMPOSITE_EXT: &'static dyn CompositeExtension = &[<$T Extension>];
        }
    };
}

use crate::compute::flatten::{FlattenFn, FlattenedArray};
pub(crate) use composite_impl;
