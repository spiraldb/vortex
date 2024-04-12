use vortex_error::VortexError;

use crate::encoding::{ArrayEncoding, EncodingRef};
use crate::encoding::{EncodingId, WithEncodedArray};
use crate::{Array, ArrayMetadata, WithTypedArray};
use crate::{ArrayTrait, TryDeserializeArrayMetadata, TrySerializeArrayMetadata};

/// Trait the defines the set of types relating to an array.
/// Because it has associated types it can't be used as a trait object.
pub trait ArrayDef {
    const ID: EncodingId;
    const ENCODING: EncodingRef;

    type Array<'a>: ArrayTrait + TryFrom<Array<'a>, Error = VortexError> + 'a;
    type Metadata: ArrayMetadata
        + Clone
        + TrySerializeArrayMetadata
        + for<'a> TryDeserializeArrayMetadata<'a>;
    type Encoding: ArrayEncoding + WithEncodedArray<D = Self> + WithTypedArray<D = Self>;
}

#[macro_export]
macro_rules! impl_encoding {
    ($id:literal, $Name:ident) => {
        use paste::paste;

        paste! {
            use $crate::{
                Array,
                ArrayDef,
                ArrayTrait,
                Flattened,
                TypedArray,
                WithTypedArray,
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

            pub type [<$Name Array>]<'a> = TypedArray<'a, [<$Name Def>]>;
            pub type [<Owned $Name Array>] = TypedArray<'static, [<$Name Def>]>;

            /// The array encoding
            pub struct [<$Name Encoding>];
            #[$crate::linkme::distributed_slice(VORTEX_ENCODINGS)]
            #[allow(non_upper_case_globals)]
            static [<ENCODINGS_ $Name>]: EncodingRef = &[<$Name Encoding>];

            impl WithEncodedArray for [<$Name Encoding>] {
                type D = [<$Name Def>];
            }
            impl WithTypedArray for [<$Name Encoding>] {
                type D = [<$Name Def>];
            }

            impl ArrayEncoding for [<$Name Encoding>] {
                fn as_any(&self) -> &dyn Any {
                    self
                }

                fn id(&self) -> EncodingId {
                    [<$Name Def>]::ID
                }

                fn flatten<'a>(&self, array: Array<'a>) -> VortexResult<Flattened<'a>> {
                    <Self as WithEncodedArray>::flatten(array)
                }

                #[inline]
                fn with_dyn<'a>(
                    &self,
                    array: &'a Array<'a>,
                    f: &mut dyn for<'b> FnMut(&'b (dyn ArrayTrait + 'a)) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    <Self as WithEncodedArray>::with_dyn(array, f)
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
        }
    };
}
