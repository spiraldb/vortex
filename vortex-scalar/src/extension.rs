use vortex_dtype::{DType, ExtDType};
use vortex_error::{vortex_bail, VortexError, VortexResult};

use crate::value::ScalarValue;
use crate::Scalar;

pub struct ExtScalar<'a> {
    dtype: &'a DType,
    // TODO(ngates): we may need to serialize the value's dtype too so we can pull
    //  it out as a scalar.
    value: &'a ScalarValue,
}

impl<'a> ExtScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.dtype
    }

    /// Returns the stored value of the extension scalar.
    pub fn value(&self) -> &'a ScalarValue {
        self.value
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }
}

impl<'a> TryFrom<&'a Scalar> for ExtScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if !matches!(value.dtype(), DType::Extension(..)) {
            vortex_bail!("Expected extension scalar, found {}", value.dtype())
        }

        Ok(Self {
            dtype: value.dtype(),
            value: &value.value,
        })
    }
}

impl Scalar {
    pub fn extension(ext_dtype: ExtDType, storage: Self) -> Self {
        Self {
            dtype: DType::Extension(ext_dtype, storage.dtype().nullability()),
            value: storage.value,
        }
    }
}
