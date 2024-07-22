use std::cmp::Ordering;

use vortex_dtype::DType;

mod binary;
mod bool;
mod datafusion;
mod display;
mod extension;
mod list;
mod primitive;
mod pvalue;
mod serde;
mod struct_;
mod utf8;
mod value;

pub use binary::*;
pub use bool::*;
pub use extension::*;
pub use list::*;
pub use primitive::*;
pub use pvalue::*;
pub use struct_::*;
pub use utf8::*;
pub use value::*;
use vortex_error::{vortex_bail, VortexResult};

#[cfg(feature = "proto")]
pub mod proto {
    #[allow(clippy::module_inception)]
    pub mod scalar {
        include!(concat!(env!("OUT_DIR"), "/proto/vortex.scalar.rs"));
    }

    pub use vortex_dtype::proto::dtype;
}

#[cfg(feature = "flatbuffers")]
pub mod flatbuffers {
    pub use generated::vortex::scalar::*;

    #[allow(clippy::all)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(unsafe_op_in_unsafe_fn)]
    #[allow(unused_imports)]
    pub mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/scalar.rs"));
    }

    mod deps {
        pub mod dtype {
            #[allow(unused_imports)]
            pub use vortex_dtype::flatbuffers as dtype;
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct Scalar {
    pub(crate) dtype: DType,
    pub(crate) value: ScalarValue,
}

impl Scalar {
    pub fn new(dtype: DType, value: ScalarValue) -> Self {
        Self { dtype, value }
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    pub fn value(&self) -> &ScalarValue {
        &self.value
    }

    #[inline]
    pub fn into_value(self) -> ScalarValue {
        self.value
    }

    pub fn is_valid(&self) -> bool {
        !self.value.is_null()
    }

    pub fn is_null(&self) -> bool {
        self.value.is_null()
    }

    pub fn null(dtype: DType) -> Self {
        assert!(dtype.is_nullable());
        Self {
            dtype,
            value: ScalarValue::Null,
        }
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Self> {
        if self.dtype() == dtype {
            return Ok(self.clone());
        }

        if self.is_null() && !dtype.is_nullable() {
            vortex_bail!("Can't cast null scalar to non-nullable type")
        }

        match dtype {
            DType::Null => vortex_bail!("Can't cast non-null to null"),
            DType::Bool(_) => BoolScalar::try_from(self).and_then(|s| s.cast(dtype)),
            DType::Primitive(..) => PrimitiveScalar::try_from(self).and_then(|s| s.cast(dtype)),
            DType::Utf8(_) => Utf8Scalar::try_from(self).and_then(|s| s.cast(dtype)),
            DType::Binary(_) => BinaryScalar::try_from(self).and_then(|s| s.cast(dtype)),
            DType::Struct(..) => StructScalar::try_from(self).and_then(|s| s.cast(dtype)),
            DType::List(..) => ListScalar::try_from(self).and_then(|s| s.cast(dtype)),
            DType::Extension(..) => ExtScalar::try_from(self).and_then(|s| s.cast(dtype)),
        }
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        self.dtype == other.dtype && self.value == other.value
    }
}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.dtype() == other.dtype() {
            self.value.partial_cmp(&other.value)
        } else {
            None
        }
    }
}

impl AsRef<Self> for Scalar {
    fn as_ref(&self) -> &Self {
        self
    }
}
