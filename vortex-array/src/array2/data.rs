use std::marker::PhantomData;
use std::sync::Arc;

use arrow_buffer::Buffer;
use vortex_error::{vortex_bail, VortexError};
use vortex_schema::DType;

use crate::array2::{Array, ArrayDef, ArrayMetadata, EncodingRef, IntoArray, ToArray};

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ArrayData {
    encoding: EncodingRef,
    dtype: DType,
    metadata: Arc<dyn ArrayMetadata>,
    buffers: Arc<[Buffer]>,
    children: Arc<[ArrayData]>,
}

impl ArrayData {
    pub fn new(
        encoding: EncodingRef,
        dtype: DType,
        metadata: Arc<dyn ArrayMetadata>,
        buffers: Arc<[Buffer]>,
        children: Arc<[ArrayData]>,
    ) -> Self {
        Self {
            encoding,
            dtype,
            metadata,
            buffers,
            children,
        }
    }

    pub fn as_typed<D: ArrayDef>(&self) -> TypedArrayData<D> {
        TypedArrayData {
            data: self.clone(),
            phantom: PhantomData,
        }
    }
}

impl ArrayData {
    pub fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn metadata(&self) -> &Arc<dyn ArrayMetadata> {
        &self.metadata
    }

    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    pub fn children(&self) -> &[ArrayData] {
        &self.children
    }
}

pub struct TypedArrayData<D: ArrayDef> {
    data: ArrayData,
    phantom: PhantomData<D>,
}

impl<D: ArrayDef> TypedArrayData<D>
where
    Self: for<'a> AsRef<D::Array<'a>>,
{
    pub fn data(&self) -> &ArrayData {
        &self.data
    }

    pub fn into_data(self) -> ArrayData {
        self.data
    }

    pub fn metadata(&self) -> &D::Metadata {
        self.data
            .metadata()
            .as_any()
            .downcast_ref::<D::Metadata>()
            .unwrap()
    }

    pub fn into_metadata(self) -> Arc<D::Metadata> {
        self.data
            .metadata
            .as_any_arc()
            .downcast::<D::Metadata>()
            .unwrap()
    }

    pub fn as_array(&self) -> &D::Array<'_> {
        self.as_ref()
    }
}

impl<D: ArrayDef> ToArray for TypedArrayData<D> {
    fn to_array(&self) -> Array {
        Array::DataRef(&self.data)
    }
}

impl<D: ArrayDef> IntoArray for TypedArrayData<D> {
    fn into_array(self) -> Array<'static> {
        Array::Data(self.data)
    }
}

impl<D: ArrayDef> TryFrom<&ArrayData> for TypedArrayData<D> {
    type Error = VortexError;

    fn try_from(data: &ArrayData) -> Result<Self, Self::Error> {
        if data.encoding().id() != D::ID {
            vortex_bail!("Invalid encoding for array")
        }
        Ok(Self {
            data: data.clone(),
            phantom: PhantomData,
        })
    }
}
