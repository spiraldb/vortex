#[macro_export]
macro_rules! impl_encoding {
    ($id:literal, $Name:ident) => {
        use paste::paste;

        paste! {
            use $crate::array2::ArrayDef;
            use $crate::encoding::EncodingId;
            use std::any::Any;
            use std::sync::Arc;
            use std::marker::{Send, Sync};

            /// The array definition trait
            pub struct [<$Name Def>];
            impl ArrayDef for [<$Name Def>] {
                const ID: EncodingId = EncodingId::new($id);
                type Array<'a> = dyn [<$Name Array>] + 'a;
                type Metadata = [<$Name Metadata>];
                type Encoding = [<$Name Encoding>];
            }

            pub type [<$Name Data>] = TypedArrayData<[<$Name Def>]>;
            pub type [<$Name View>]<'v> = TypedArrayView<'v, [<$Name Def>]>;

            /// The array encoding
            pub struct [<$Name Encoding>];
            impl ArrayEncoding for [<$Name Encoding>] {
                fn id(&self) -> EncodingId {
                    [<$Name Def>]::ID
                }

                fn with_view_mut<'v>(
                    &self,
                    view: &'v ArrayView<'v>,
                    f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    // Convert ArrayView -> PrimitiveArray, then call compute.
                    let typed_view: TypedArrayView<'v, [<$Name Def>]> = TypedArrayView::try_from(view)?;
                    f(&typed_view.as_array())
                }

                fn with_data_mut(
                    &self,
                    data: &ArrayData,
                    f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    let data = TypedArrayData::<[<$Name Def>]>::try_from(data)?;
                    f(&data.as_array())
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

            /// Implement AsRef for both the data and view types
            impl<'a> AsRef<dyn [<$Name Array>] + 'a> for [<$Name Data>] {
                fn as_ref(&self) -> &(dyn [<$Name Array>] + 'a) {
                    self
                }
            }
            impl<'a> AsRef<dyn [<$Name Array>] + 'a> for [<$Name View>]<'a> {
                fn as_ref(&self) -> &(dyn [<$Name Array>] + 'a) {
                    self
                }
            }
        }
    };
}
