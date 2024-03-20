use std::fmt::Debug;
use std::sync::Arc;

use crate::array::composite::array::CompositeArray;
use crate::array::composite::{CompositeID, CompositeMetadata};
use crate::array::{Array, ArrayRef};
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::error::VortexResult;

pub trait CompositeExtension: Debug + Send + Sync + 'static {
    fn id(&self) -> CompositeID;

    fn as_typed_compute(&self, array: &CompositeArray) -> Box<dyn ArrayCompute>;
}

pub type CompositeExtensionRef = &'static dyn CompositeExtension;

#[derive(Debug, Clone)]
pub struct TypedCompositeArray<M: CompositeMetadata> {
    metadata: M,
    underlying: ArrayRef,
}

impl<M: CompositeMetadata> TypedCompositeArray<M> {
    pub fn new(metadata: M, underlying: ArrayRef) -> Self {
        Self {
            metadata,
            underlying,
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

    pub fn as_composite(&self) -> CompositeArray {
        CompositeArray::new(
            self.id(),
            Arc::new(self.metadata().serialize()),
            dyn_clone::clone_box(self.underlying()),
        )
    }
}

impl<M: CompositeMetadata> FlattenFn for TypedCompositeArray<M> {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Composite(self.as_composite()))
    }
}

macro_rules! composite_impl {
    ($id:expr, $T:ty) => {
        use crate::array::composite::array::{CompositeArray, CompositeMetadata};
        use crate::array::composite::typed::CompositeExtension;
        use crate::array::composite::COMPOSITE_EXTENSIONS;
        use crate::array::ArrayCompute;
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

                fn as_typed_compute(&self, array: &CompositeArray) -> Box<dyn ArrayCompute> {
                    if array.id() != Self::ID {
                        panic!("Incorrect CompositeID");
                    }
                    Box::new(array.as_typed::<$T>())
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

use crate::compute::ArrayCompute;
pub(crate) use composite_impl;
