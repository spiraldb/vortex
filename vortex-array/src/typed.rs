use std::sync::Arc;

use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError, VortexResult};

use crate::stats::StatsSet;
use crate::{Array, ArrayData, ArrayDef, AsArray, IntoArray, ToArray, TryDeserializeArrayMetadata};

#[derive(Debug, Clone)]
pub struct TypedArray<D: ArrayDef> {
    array: Array,
    metadata: D::Metadata,
}

impl<D: ArrayDef> TypedArray<D> {
    pub fn try_from_parts(
        dtype: DType,
        len: usize,
        metadata: D::Metadata,
        buffer: Option<Buffer>,
        children: Arc<[Array]>,
        stats: StatsSet,
    ) -> VortexResult<Self> {
        let array = Array::Data(ArrayData::try_new(
            D::ENCODING,
            dtype,
            len,
            Arc::new(metadata.clone()),
            buffer,
            children,
            stats,
        )?);
        Ok(Self { array, metadata })
    }

    pub fn metadata(&self) -> &D::Metadata {
        &self.metadata
    }
}

impl<D: ArrayDef> TypedArray<D> {
    pub fn array(&self) -> &Array {
        &self.array
    }
}

impl<D: ArrayDef> TryFrom<Array> for TypedArray<D> {
    type Error = VortexError;

    fn try_from(array: Array) -> Result<Self, Self::Error> {
        if array.encoding().id() != D::ENCODING.id() {
            vortex_bail!(
                "incorrect encoding {}, expected {}",
                array.encoding().id().as_ref(),
                D::ENCODING.id().as_ref(),
            );
        }
        let metadata = match &array {
            Array::Data(d) => d
                .metadata()
                .as_any()
                .downcast_ref::<D::Metadata>()
                .unwrap()
                .clone(),
            Array::View(v) => D::Metadata::try_deserialize_metadata(v.metadata())?,
        };
        Ok(Self { array, metadata })
    }
}

impl<'a, D: ArrayDef> TryFrom<&'a Array> for TypedArray<D> {
    type Error = VortexError;

    fn try_from(value: &'a Array) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl<D: ArrayDef> AsArray for TypedArray<D> {
    fn as_array_ref(&self) -> &Array {
        &self.array
    }
}

impl<D: ArrayDef> ToArray for TypedArray<D> {
    fn to_array(&self) -> Array {
        self.array.clone()
    }
}

impl<D: ArrayDef> IntoArray for TypedArray<D> {
    fn into_array(self) -> Array {
        self.array
    }
}
