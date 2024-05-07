use vortex_dtype::DType;
use vortex_dtype::Nullability::NonNullable;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::value::{ScalarData, ScalarValue};
use crate::Scalar;

pub struct BoolScalar<'a>(&'a Scalar);
impl<'a> BoolScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    pub fn value(&self) -> Option<bool> {
        self.0.value.as_bool()
    }
}

impl<'a> TryFrom<&'a Scalar> for BoolScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if matches!(value.dtype(), DType::Bool(_)) {
            Ok(Self(value))
        } else {
            vortex_bail!("Expected bool scalar, found {}", value.dtype())
        }
    }
}

impl TryFrom<&Scalar> for bool {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> VortexResult<Self> {
        BoolScalar::try_from(value)?
            .value()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn into_from() {
        let scalar: Scalar = false.into();
        assert!(!bool::try_from(&scalar).unwrap());
    }
}
