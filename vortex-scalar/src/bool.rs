use vortex_dtype::Nullability::NonNullable;
use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::value::ScalarValue;
use crate::Scalar;

pub struct BoolScalar<'a> {
    dtype: &'a DType,
    value: Option<bool>,
}

impl<'a> BoolScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.dtype
    }

    pub fn value(&self) -> Option<bool> {
        self.value
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        match dtype {
            DType::Bool(_) => Ok(Scalar::bool(
                self.value().ok_or_else(|| vortex_err!("not a bool"))?,
                dtype.nullability(),
            )),
            _ => vortex_bail!("Can't cast {} to bool", dtype),
        }
    }
}

impl Scalar {
    pub fn bool(value: bool, nullability: Nullability) -> Self {
        Self {
            dtype: DType::Bool(nullability),
            value: ScalarValue::Bool(value),
        }
    }
}

impl<'a> TryFrom<&'a Scalar> for BoolScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if !matches!(value.dtype(), DType::Bool(_)) {
            vortex_bail!("Expected bool scalar, found {}", value.dtype())
        }
        Ok(Self {
            dtype: value.dtype(),
            value: value.value.as_bool()?,
        })
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
        Self {
            dtype: DType::Bool(NonNullable),
            value: value.into(),
        }
    }
}

impl TryFrom<&ScalarValue> for bool {
    type Error = VortexError;

    fn try_from(value: &ScalarValue) -> VortexResult<Self> {
        value
            .as_bool()?
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}

impl From<bool> for ScalarValue {
    fn from(value: bool) -> Self {
        ScalarValue::Bool(value)
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
