use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;
use std::any::Any;
use std::fmt::{Display, Formatter};

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
    fn boxed(self) -> Box<dyn Scalar> {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &DType::Bool
    }

    fn cast(&self, dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        match dtype {
            DType::Bool => Ok(Box::new(self.clone())),
            _ => Err(EncError::InvalidDType(dtype.clone())),
        }
    }

    fn nbytes(&self) -> usize {
        1
    }
}

impl From<bool> for Box<dyn Scalar> {
    #[inline]
    fn from(value: bool) -> Self {
        Box::new(BoolScalar::new(value))
    }
}

impl TryFrom<Box<dyn Scalar>> for bool {
    type Error = EncError;

    #[inline]
    fn try_from(value: Box<dyn Scalar>) -> EncResult<Self> {
        value.as_ref().try_into()
    }
}

impl TryFrom<&dyn Scalar> for bool {
    type Error = EncError;

    fn try_from(value: &dyn Scalar) -> EncResult<Self> {
        match value.as_any().downcast_ref::<BoolScalar>() {
            Some(bool_scalar) => Ok(bool_scalar.value()),
            None => Err(EncError::InvalidDType(value.dtype().clone())),
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
        let scalar: Box<dyn Scalar> = false.into();
        assert_eq!(scalar.as_ref().try_into(), Ok(false));
    }
}
