use std::any::Any;
use std::fmt::{Display, Formatter};

use crate::dtype::{DType, Nullability};
use crate::error::{VortexError, VortexResult};
use crate::scalar::{NullableScalar, Scalar, ScalarRef};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct BoolScalar {
    value: bool,
}

impl BoolScalar {
    pub fn new(value: bool) -> Self {
        Self { value }
    }

    pub fn value(&self) -> bool {
        self.value
    }
}

impl Scalar for BoolScalar {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    #[inline]
    fn as_nonnull(&self) -> Option<&dyn Scalar> {
        Some(self)
    }

    #[inline]
    fn into_nonnull(self: Box<Self>) -> Option<ScalarRef> {
        Some(self)
    }

    #[inline]
    fn boxed(self) -> ScalarRef {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &DType::Bool(Nullability::NonNullable)
    }

    fn cast(&self, dtype: &DType) -> VortexResult<ScalarRef> {
        match dtype {
            DType::Bool(Nullability::NonNullable) => Ok(self.clone().boxed()),
            DType::Bool(Nullability::Nullable) => {
                Ok(NullableScalar::some(self.clone().boxed()).boxed())
            }
            _ => Err(VortexError::InvalidDType(dtype.clone())),
        }
    }

    fn nbytes(&self) -> usize {
        1
    }
}

impl From<bool> for ScalarRef {
    #[inline]
    fn from(value: bool) -> Self {
        BoolScalar::new(value).boxed()
    }
}

impl TryFrom<ScalarRef> for bool {
    type Error = VortexError;

    #[inline]
    fn try_from(value: ScalarRef) -> VortexResult<Self> {
        value.as_ref().try_into()
    }
}

impl TryFrom<&dyn Scalar> for bool {
    type Error = VortexError;

    fn try_from(value: &dyn Scalar) -> VortexResult<Self> {
        if let Some(bool_scalar) = value
            .as_nonnull()
            .and_then(|v| v.as_any().downcast_ref::<BoolScalar>())
        {
            Ok(bool_scalar.value())
        } else {
            Err(VortexError::InvalidDType(value.dtype().clone()))
        }
    }
}

impl Display for BoolScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn into_from() {
        let scalar: ScalarRef = false.into();
        assert_eq!(scalar.as_ref().try_into(), Ok(false));
    }
}
