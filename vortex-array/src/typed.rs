use std::sync::{Arc, OnceLock};

use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_panic, VortexError, VortexResult};

use crate::stats::StatsSet;
use crate::{Array, ArrayData, ArrayDef, IntoArray, ToArray, TryDeserializeArrayMetadata};

#[derive(Debug, Clone)]
pub struct TypedArray<D: ArrayDef> {
    array: Array,
    lazy_metadata: OnceLock<D::Metadata>,
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
            Arc::new(metadata),
            buffer,
            children,
            stats,
        )?);
        Ok(Self {
            array,
            lazy_metadata: OnceLock::new(),
        })
    }

    pub fn metadata(&self) -> &D::Metadata {
        match &self.array {
            Array::Data(d) => d
                .metadata()
                .as_any()
                .downcast_ref::<D::Metadata>()
                .unwrap_or_else(|| {
                    vortex_panic!(
                        "Failed to downcast metadata to {} for typed array with ID {} and encoding {}",
                        std::any::type_name::<D::Metadata>(),
                        D::ID.as_ref(),
                        D::ENCODING.id().as_ref(),
                    )
                }),
            Array::View(v) => self
                .lazy_metadata
                .get_or_init(|| {
                    D::Metadata::try_deserialize_metadata(v.metadata()).unwrap_or_else(|err| {
                        vortex_panic!(
                            "Failed to deserialize ArrayView metadata for typed array with ID {} and encoding {}: {}", 
                            D::ID.as_ref(),
                            D::ENCODING.id().as_ref(),
                            err
                        )
                    })
                }),
        }
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
        Ok(Self {
            array,
            lazy_metadata: OnceLock::new(),
        })
    }
}

impl<'a, D: ArrayDef> TryFrom<&'a Array> for TypedArray<D> {
    type Error = VortexError;

    fn try_from(value: &'a Array) -> Result<Self, Self::Error> {
        value.clone().try_into()
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
