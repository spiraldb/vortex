use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError, VortexExpect as _, VortexResult};

use crate::encoding::{ArrayEncoding, ArrayEncodingExt, ArrayEncodingRef, EncodingId, EncodingRef};
use crate::stats::{ArrayStatistics, Statistics};
use crate::visitor::ArrayVisitor;
use crate::{
    Array, ArrayDType, ArrayData, ArrayLen, ArrayMetadata, ArrayTrait, GetArrayMetadata, IntoArray,
    ToArrayData, TryDeserializeArrayMetadata,
};

/// Trait the defines the set of types relating to an array.
/// Because it has associated types it can't be used as a trait object.
pub trait ArrayDef {
    const ID: EncodingId;
    const ENCODING: EncodingRef;

    type Array: ArrayTrait + TryFrom<Array, Error = VortexError>;
    type Metadata: ArrayMetadata + Clone + for<'m> TryDeserializeArrayMetadata<'m>;
    type Encoding: ArrayEncoding + ArrayEncodingExt<D = Self>;
}

#[macro_export]
macro_rules! impl_encoding {
    ($id:literal, $code:literal, $Name:ident) => {
        $crate::vendored::paste::paste! {
            /// The array definition trait
            #[derive(std::fmt::Debug, Clone)]
            pub struct $Name;
            impl $crate::ArrayDef for $Name {
                const ID: $crate::encoding::EncodingId = $crate::encoding::EncodingId::new($id, $code);
                const ENCODING: $crate::encoding::EncodingRef = &[<$Name Encoding>];
                type Array = [<$Name Array>];
                type Metadata = [<$Name Metadata>];
                type Encoding = [<$Name Encoding>];
            }

            #[derive(std::fmt::Debug, Clone)]
            pub struct [<$Name Array>] {
                typed: $crate::TypedArray<$Name>
            }
            impl AsRef<$crate::Array> for [<$Name Array>] {
                fn as_ref(&self) -> &$crate::Array {
                    self.typed.array()
                }
            }
            impl [<$Name Array>] {
                #[allow(clippy::same_name_method)]
                fn metadata(&self) -> &[<$Name Metadata>] {
                    self.typed.metadata()
                }

                pub fn len(&self) -> usize {
                    self.typed.array().len()
                }

                pub fn is_empty(&self) -> bool {
                    self.typed.array().is_empty()
                }

                #[allow(dead_code)]
                fn try_from_parts(
                    dtype: vortex_dtype::DType,
                    len: usize,
                    metadata: [<$Name Metadata>],
                    children: std::sync::Arc<[$crate::Array]>,
                    stats: $crate::stats::StatsSet,
                ) -> VortexResult<Self> {
                    Ok(Self { typed: $crate::TypedArray::try_from_parts(dtype, len, metadata, None, children, stats)? })
                }
            }
            impl $crate::GetArrayMetadata for [<$Name Array>] {
                #[allow(clippy::same_name_method)]
                fn metadata(&self) -> std::sync::Arc<dyn $crate::ArrayMetadata> {
                    std::sync::Arc::new(self.metadata().clone())
                }
            }
            impl $crate::ToArray for [<$Name Array>] {
                fn to_array(&self) -> $crate::Array {
                    self.typed.to_array()
                }
            }
            impl<'a> $crate::IntoArray for [<$Name Array>] {
                fn into_array(self) -> $crate::Array {
                    self.typed.into_array()
                }
            }
            impl From<$crate::TypedArray<$Name>> for [<$Name Array>] {
                fn from(typed: $crate::TypedArray<$Name>) -> Self {
                    Self { typed }
                }
            }
            impl TryFrom<$crate::Array> for [<$Name Array>] {
                type Error = vortex_error::VortexError;

                #[inline]
                fn try_from(array: $crate::Array) -> Result<Self, Self::Error> {
                    $crate::TypedArray::<$Name>::try_from(array).map(Self::from)
                }
            }
            impl TryFrom<&$crate::Array> for [<$Name Array>] {
                type Error = vortex_error::VortexError;

                #[inline]
                fn try_from(array: &$crate::Array) -> Result<Self, Self::Error> {
                    $crate::TypedArray::<$Name>::try_from(array).map(Self::from)
                }
            }
            impl From<[<$Name Array>]> for $crate::Array {
                fn from(value: [<$Name Array>]) -> $crate::Array {
                    use $crate::IntoArray;
                    value.typed.into_array()
                }
            }

            /// The array encoding
            #[derive(std::fmt::Debug)]
            pub struct [<$Name Encoding>];
            impl $crate::encoding::ArrayEncoding for [<$Name Encoding>] {
                #[inline]
                fn id(&self) -> $crate::encoding::EncodingId {
                    $Name::ID
                }

                #[inline]
                fn canonicalize(&self, array: $crate::Array) -> vortex_error::VortexResult<$crate::Canonical> {
                    <Self as $crate::encoding::ArrayEncodingExt>::into_canonical(array)
                }

                #[inline]
                fn with_dyn(
                    &self,
                    array: &$crate::Array,
                    f: &mut dyn for<'b> FnMut(&'b (dyn $crate::ArrayTrait + 'b)) -> vortex_error::VortexResult<()>,
                ) -> vortex_error::VortexResult<()> {
                    <Self as $crate::encoding::ArrayEncodingExt>::with_dyn(array, f)
                }
            }
            impl $crate::encoding::ArrayEncodingExt for [<$Name Encoding>] {
                type D = $Name;
            }

            /// Implement ArrayMetadata
            impl $crate::ArrayMetadata for [<$Name Metadata>] {
                #[inline]
                fn as_any(&self) -> &dyn std::any::Any {
                    self
                }

                #[inline]
                fn as_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + std::marker::Send + std::marker::Sync> {
                    self
                }
            }
        }
    };
}

impl<T: AsRef<Array>> ArrayEncodingRef for T {
    fn encoding(&self) -> EncodingRef {
        self.as_ref().encoding()
    }
}

impl<T: AsRef<Array>> ArrayDType for T {
    fn dtype(&self) -> &DType {
        match self.as_ref() {
            Array::Data(d) => d.dtype(),
            Array::View(v) => v.dtype(),
        }
    }
}

impl<T: AsRef<Array>> ArrayLen for T {
    fn len(&self) -> usize {
        match self.as_ref() {
            Array::Data(d) => d.len(),
            Array::View(v) => v.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self.as_ref() {
            Array::Data(d) => d.is_empty(),
            Array::View(v) => v.is_empty(),
        }
    }
}

impl<T: AsRef<Array>> ArrayStatistics for T {
    fn statistics(&self) -> &(dyn Statistics + '_) {
        match self.as_ref() {
            Array::Data(d) => d.statistics(),
            Array::View(v) => v.statistics(),
        }
    }
}

impl<D> ToArrayData for D
where
    D: IntoArray + ArrayEncodingRef + ArrayStatistics + GetArrayMetadata + Clone,
{
    fn to_array_data(&self) -> ArrayData {
        // TODO: move metadata call `Array::View` match
        let metadata = self.metadata();
        let array = self.clone().into_array();
        match array {
            Array::Data(d) => d,
            Array::View(ref view) => {
                let encoding = view.encoding();
                let stats = view.statistics().to_set();
                struct Visitor {
                    buffer: Option<Buffer>,
                    children: Vec<Array>,
                }
                impl ArrayVisitor for Visitor {
                    fn visit_child(&mut self, _name: &str, array: &Array) -> VortexResult<()> {
                        self.children.push(array.clone());
                        Ok(())
                    }

                    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()> {
                        if self.buffer.is_some() {
                            vortex_bail!("Multiple buffers found in view")
                        }
                        self.buffer = Some(buffer.clone());
                        Ok(())
                    }
                }
                let mut visitor = Visitor {
                    buffer: None,
                    children: vec![],
                };
                array.with_dyn(|a| {
                    a.accept(&mut visitor)
                        .vortex_expect("Error while visiting Array View children")
                });
                ArrayData::try_new(
                    encoding,
                    array.dtype().clone(),
                    array.len(),
                    metadata,
                    visitor.buffer,
                    visitor.children.into(),
                    stats,
                )
                .vortex_expect("Failed to create ArrayData from Array View")
            }
        }
    }
}
