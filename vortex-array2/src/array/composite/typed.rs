use std::fmt::Debug;

use vortex_error::VortexResult;
use vortex_schema::CompositeID;
use vortex_schema::DType;

use crate::array::composite::array::CompositeArray;
use crate::array::composite::UnderlyingMetadata;
use crate::compute::ArrayCompute;
use crate::Array;

pub trait CompositeExtension: Debug + Send + Sync + 'static {
    fn id(&self) -> CompositeID;

    fn with_compute<'a>(
        &self,
        array: &'a CompositeArray<'a>,
        f: &mut dyn for<'b> FnMut(&'b (dyn ArrayCompute + 'a)) -> VortexResult<()>,
    ) -> VortexResult<()>;
}

pub type CompositeExtensionRef = &'static dyn CompositeExtension;

#[derive(Debug, Clone)]
pub struct TypedCompositeArray<'a, M: UnderlyingMetadata> {
    metadata: M,
    underlying: Array<'a>,
    dtype: DType,
}

impl<'a, M: UnderlyingMetadata> TypedCompositeArray<'a, M> {
    pub fn new(metadata: M, underlying: Array<'a>) -> Self {
        let dtype = DType::Composite(metadata.id(), underlying.dtype().is_nullable().into());
        Self {
            metadata,
            underlying,
            dtype,
        }
    }

    #[inline]
    pub fn underlying_metadata(&self) -> &M {
        &self.metadata
    }

    #[inline]
    pub fn underlying(&self) -> &Array<'a> {
        &self.underlying
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn as_composite(&self) -> VortexResult<CompositeArray<'a>> {
        Ok(CompositeArray::new(
            self.underlying_metadata().id(),
            self.underlying_metadata().try_serialize_metadata()?,
            self.underlying().clone(),
        ))
    }
}

#[macro_export]
macro_rules! impl_composite {
    ($id:expr, $T:ty) => {
        use linkme::distributed_slice;
        use paste::paste;
        use vortex_schema::{CompositeID, DType, Nullability};
        use $crate::array::composite::{
            CompositeArray, CompositeExtension, TypedCompositeArray, UnderlyingMetadata,
            VORTEX_COMPOSITE_EXTENSIONS,
        };
        use $crate::compute::ArrayCompute;
        use $crate::TryDeserializeArrayMetadata;

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

                fn with_compute<'a>(
                    &self,
                    array: &'a CompositeArray<'a>,
                    f: &mut dyn for<'b> FnMut(&'b (dyn ArrayCompute + 'a)) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    if array.id() != Self::ID {
                        panic!("Incorrect CompositeID");
                    }
                    let typed = TypedCompositeArray::new(
                        $T::try_deserialize_metadata(Some(array.underlying_metadata().as_ref()))?,
                        array.underlying().clone(),
                    );
                    f(&typed)
                }
            }

            impl UnderlyingMetadata for $T {
                fn id(&self) -> CompositeID {
                    [<$T Extension>]::ID
                }
            }

            #[distributed_slice(VORTEX_COMPOSITE_EXTENSIONS)]
            static ENCODINGS_COMPOSITE_EXT: &'static dyn CompositeExtension = &[<$T Extension>];
        }
    };
}

pub use impl_composite;
