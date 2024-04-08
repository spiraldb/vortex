use crate::encoding::EncodingId;
use crate::encoding::{ArrayEncoding, EncodingRef};
use crate::{ArrayMetadata, TryFromArrayParts};
use crate::{ArrayTrait, TryDeserializeArrayMetadata, TrySerializeArrayMetadata};

/// Trait the defines the set of types relating to an array.
/// Because it has associated types it can't be used as a trait object.
pub trait ArrayDef {
    const ID: EncodingId;
    const ENCODING: EncodingRef;

    type Array<'a>: ArrayTrait + TryFromArrayParts<'a, Self::Metadata> + 'a;
    type Metadata: ArrayMetadata
        + TrySerializeArrayMetadata
        + for<'a> TryDeserializeArrayMetadata<'a>;
    type Encoding: ArrayEncoding;
}

#[macro_export]
macro_rules! impl_encoding {
    ($id:literal, $Name:ident) => {
        use paste::paste;

        paste! {
            use $crate::{
                ArrayDef, ArrayParts, ArrayTrait, TryFromArrayParts,
                TryDeserializeArrayMetadata,
            };
            use $crate::encoding::{ArrayEncoding, EncodingId, EncodingRef};
            use vortex_error::vortex_err;
            use std::any::Any;
            use std::fmt::Debug;
            use std::sync::Arc;
            use std::marker::{Send, Sync};

            /// The array definition trait
            #[derive(Debug)]
            pub struct [<$Name Def>];
            impl ArrayDef for [<$Name Def>] {
                const ID: EncodingId = EncodingId::new($id);
                const ENCODING: EncodingRef = &[<$Name Encoding>];
                type Array<'a> = [<$Name Array>]<'a>;
                type Metadata = [<$Name Metadata>];
                type Encoding = [<$Name Encoding>];
            }

            pub type [<$Name Data>] = TypedArrayData<[<$Name Def>]>;

            /// The array encoding
            pub struct [<$Name Encoding>];
            impl ArrayEncoding for [<$Name Encoding>] {
                fn id(&self) -> EncodingId {
                    [<$Name Def>]::ID
                }

                fn with_view_mut<'v>(
                    &self,
                    view: &'v ArrayView<'v>,
                    f: &mut dyn FnMut(&dyn ArrayTrait) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    // Convert ArrayView -> PrimitiveArray, then call compute.
                    let metadata = [<$Name Metadata>]::try_deserialize_metadata(view.metadata())?;
                    let array = [<$Name Array>]::try_from_parts(view as &dyn ArrayParts, &metadata)?;
                    f(&array)
                }

                fn with_data_mut(
                    &self,
                    data: &ArrayData,
                    f: &mut dyn FnMut(&dyn ArrayTrait) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    let metadata = data.metadata()
                        .as_any()
                        .downcast_ref::<[<$Name Metadata>]>()
                        .ok_or_else(|| vortex_err!("Failed to downcast metadata"))?
                        .clone();
                    let array = [<$Name Array>]::try_from_parts(data as &dyn ArrayParts, &metadata)?;
                    f(&array)
                }
            }

            /// Implement ArrayMetadata
            impl ArrayMetadata for [<$Name Metadata>] {
                fn as_any(&self) -> &dyn Any {
                    self
                }

                fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
                    self
                }

                fn to_arc(&self) -> Arc<dyn ArrayMetadata> {
                    Arc::new(self.clone())
                }

                fn into_arc(self) -> Arc<dyn ArrayMetadata> {
                    Arc::new(self)
                }
            }

            // /// Implement AsRef for both the data and view types
            // impl<'a> AsRef<[<$Name Array>]<'a>> for [<$Name Data>] {
            //     fn as_ref(&self) -> &[<$Name Array>]<'a> {
            //         self
            //     }
            // }
            // impl<'a> AsRef<[<$Name Array>]<'a>> for [<$Name View>]<'a> {
            //     fn as_ref(&self) -> &[<$Name Array>]<'a> {
            //         self
            //     }
            // }
        }
    };
}
