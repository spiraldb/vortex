#![allow(dead_code)]

use serde::Deserialize;
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError, VortexResult};

pub struct Scalar {
    dtype: DType,
    buffer: Buffer,
}

impl Scalar {
    pub(crate) fn flexbuffer(&self) -> VortexResult<flexbuffers::Reader<&[u8]>> {
        Ok(flexbuffers::Reader::get_root(self.buffer.as_ref())?)
    }
}

pub struct BoolScalar {
    dtype: DType,
    value: Option<bool>,
}

impl TryFrom<Scalar> for BoolScalar {
    type Error = VortexError;

    fn try_from(value: Scalar) -> Result<Self, Self::Error> {
        if !matches!(&value.dtype, &DType::Bool(_)) {
            vortex_bail!(MismatchedTypes: "bool", &value.dtype);
        }
        Ok(Self {
            dtype: value.dtype.clone(),
            value: Option::<bool>::deserialize(value.flexbuffer()?)?,
        })
    }
}
