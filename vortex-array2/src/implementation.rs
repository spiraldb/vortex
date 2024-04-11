use crate::encoding::{ArrayEncoding, EncodingRef};
use crate::encoding::{EncodingId, WithEncodedArray};
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
    type Encoding: ArrayEncoding + WithEncodedArray<D = Self>;
}

#[macro_export]
macro_rules! impl_encoding {
    ($id:literal, $Name:ident) => {
        use paste::paste;

        paste! {
            use $crate::{
                ArrayDef,
                ArrayEncodingRef,
                ArrayParts,
                ArrayTrait,
                TryFromArrayParts,
            };
            use $crate::encoding::{
                ArrayEncoding,
                EncodingId,
                EncodingRef,
                WithEncodedArray,
                VORTEX_ENCODINGS,
            };
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
            #[$crate::linkme::distributed_slice(VORTEX_ENCODINGS)]
            #[allow(non_upper_case_globals)]
            static [<ENCODINGS_ $Name>]: EncodingRef = &[<$Name Encoding>];

            impl WithEncodedArray for [<$Name Encoding>] {
                type D = [<$Name Def>];
            }

            impl ArrayEncoding for [<$Name Encoding>] {
                fn as_any(&self) -> &dyn Any {
                    self
                }

                fn id(&self) -> EncodingId {
                    [<$Name Def>]::ID
                }

                #[inline]
                fn with_view_mut<'v>(
                    &self,
                    view: &'v ArrayView<'v>,
                    f: &mut dyn FnMut(&dyn ArrayTrait) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    WithEncodedArray::with_view_mut(self, view, |a| f(a))
                }

                #[inline]
                fn with_data_mut(
                    &self,
                    data: &ArrayData,
                    f: &mut dyn FnMut(&dyn ArrayTrait) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    WithEncodedArray::with_data_mut(self, data, |a| f(a))
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
            }

            impl ArrayEncodingRef for [<$Name Array>]<'_> {
                fn encoding(&self) -> EncodingRef {
                    [<$Name Def>]::ENCODING
                }
            }
        }
    };
}
