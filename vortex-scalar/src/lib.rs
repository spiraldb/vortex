use value::*;
use vortex_dtype::DType;
use vortex_dtype::Nullability::NonNullable;

mod binary;
mod bool;
mod display;
mod extension;
mod list;
mod primitive;
mod struct_;
mod utf8;
mod value;

pub use binary::*;
pub use bool::*;
pub use extension::*;
pub use list::*;
pub use primitive::*;
pub use struct_::*;
pub use utf8::*;

#[derive(Debug, Clone)]
pub struct Scalar {
    pub(crate) dtype: DType,
    pub(crate) value: ScalarValue,
}

impl Scalar {
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn is_null(&self) -> bool {
        self.value.is_null()
    }

    pub fn null(&self, dtype: DType) -> Self {
        assert!(dtype.is_nullable());
        Self {
            dtype,
            value: ScalarValue::Data(ScalarData::None),
        }
    }
}

impl From<bool> for Scalar {
    fn from(value: bool) -> Self {
        Scalar {
            dtype: DType::Bool(NonNullable),
            value: ScalarValue::Data(ScalarData::Bool(value)),
        }
    }
}
