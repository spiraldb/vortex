use vortex::array::primitive::PrimitiveEncoding;
use vortex_error::{vortex_err, VortexError};

use crate::array::primitive::PrimitiveDef;
use crate::{Array, ArrayDef, TryDeserializeArrayMetadata};

#[derive(Debug)]
pub struct TypedArray<'a, D: ArrayDef> {
    array: Array<'a>,
    metadata: D::Metadata,
}

impl<D: ArrayDef> TypedArray<'_, D> {
    pub fn array(&self) -> &Array {
        &self.array
    }

    pub fn metadata(&self) -> &D::Metadata {
        &self.metadata
    }
}

impl<D: ArrayDef> Clone for TypedArray<'_, D> {
    fn clone(&self) -> Self {
        Self {
            array: self.array.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

impl<'a, D: ArrayDef> TryFrom<Array<'a>> for TypedArray<'a, D> {
    type Error = VortexError;

    fn try_from(array: Array<'a>) -> Result<Self, Self::Error> {
        if array.encoding().id() != D::ENCODING.id() {
            return Err(vortex_err!("incorrect encoding"));
        }
        let metadata = match &array {
            Array::Data(d) => d
                .metadata()
                .as_any()
                .downcast_ref::<D::Metadata>()
                .unwrap()
                .clone(),
            Array::DataRef(d) => d
                .metadata()
                .as_any()
                .downcast_ref::<D::Metadata>()
                .unwrap()
                .clone(),
            Array::View(v) => D::Metadata::try_deserialize_metadata(v.metadata())?,
        };
        Ok(TypedArray { array, metadata })
    }
}

pub trait WithTypedArray {
    type D: ArrayDef;

    fn with_typed_array<'a, R, F>(array: &'a Array<'a>, mut f: F) -> R
    where
        F: FnMut(&TypedArray<'a, Self::D>) -> R,
    {
        let typed = TryFrom::<Array>::try_from(array.clone()).unwrap();
        f(&typed)
    }
}

impl WithTypedArray for PrimitiveEncoding {
    type D = PrimitiveDef;
}
