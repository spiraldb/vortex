use vortex_dtype::{DType, ExtDType};
use vortex_error::{vortex_bail, VortexError};

use crate::value::ScalarValue;
use crate::Scalar;

pub struct ExtScalar<'a>(&'a Scalar);
impl<'a> ExtScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    /// Returns the stored value of the extension scalar.
    pub fn value(&self) -> &ScalarValue {
        &self.0.value
    }
}

impl<'a> TryFrom<&'a Scalar> for ExtScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if matches!(value.dtype(), DType::Extension(..)) {
            Ok(Self(value))
        } else {
            vortex_bail!("Expected extension scalar, found {}", value.dtype())
        }
    }
}

impl Scalar {
    pub fn extension(ext_dtype: ExtDType, storage: Scalar) -> Self {
        Scalar {
            dtype: DType::Extension(ext_dtype, storage.dtype().nullability()),
            value: storage.value,
        }
    }
}
