use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::{DType, Nullability};
use std::any::Any;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct BinaryScalar {
    value: Vec<u8>,
}

impl BinaryScalar {
    pub fn new(value: Vec<u8>) -> Self {
        Self { value }
    }

    pub fn value(&self) -> &Vec<u8> {
        &self.value
    }
}

impl Scalar for BinaryScalar {
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
        &DType::Binary(Nullability::NonNullable)
    }

    fn cast(&self, _dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        todo!()
    }

    fn nbytes(&self) -> usize {
        self.value.len()
    }
}

impl From<Vec<u8>> for Box<dyn Scalar> {
    fn from(value: Vec<u8>) -> Self {
        BinaryScalar::new(value).boxed()
    }
}

impl TryFrom<Box<dyn Scalar>> for Vec<u8> {
    type Error = EncError;

    fn try_from(value: Box<dyn Scalar>) -> Result<Self, Self::Error> {
        let dtype = value.dtype().clone();
        let scalar = value
            .into_any()
            .downcast::<BinaryScalar>()
            .map_err(|_| EncError::InvalidDType(dtype))?;
        Ok(scalar.value)
    }
}

impl TryFrom<&dyn Scalar> for Vec<u8> {
    type Error = EncError;

    fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
        if let Some(scalar) = value.as_any().downcast_ref::<BinaryScalar>() {
            Ok(scalar.value.clone())
        } else {
            Err(EncError::InvalidDType(value.dtype().clone()))
        }
    }
}

impl Display for BinaryScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "bytes[{}]", self.value.len())
    }
}
