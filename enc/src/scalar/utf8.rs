use std::any::Any;
use std::fmt::{Display, Formatter};

use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Utf8Scalar {
    value: String,
}

impl Utf8Scalar {
    pub fn new(value: String) -> Self {
        Self { value }
    }

    pub fn value(&self) -> &str {
        self.value.as_str()
    }
}

impl Scalar for Utf8Scalar {
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
        &DType::Utf8
    }

    fn cast(&self, _dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        todo!()
    }

    fn nbytes(&self) -> usize {
        self.value.len()
    }
}

impl From<String> for Box<dyn Scalar> {
    fn from(value: String) -> Self {
        Utf8Scalar::new(value).boxed()
    }
}

impl From<&str> for Box<dyn Scalar> {
    fn from(value: &str) -> Self {
        Utf8Scalar::new(value.to_string()).boxed()
    }
}

impl TryFrom<Box<dyn Scalar>> for String {
    type Error = EncError;

    fn try_from(value: Box<dyn Scalar>) -> Result<Self, Self::Error> {
        let dtype = value.dtype().clone();
        let scalar = value
            .into_any()
            .downcast::<Utf8Scalar>()
            .map_err(|_| EncError::InvalidDType(dtype))?;
        Ok(scalar.value)
    }
}

impl TryFrom<&dyn Scalar> for String {
    type Error = EncError;

    fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
        if let Some(scalar) = value.as_any().downcast_ref::<Utf8Scalar>() {
            Ok(scalar.value().to_string())
        } else {
            Err(EncError::InvalidDType(value.dtype().clone()))
        }
    }
}

impl Display for Utf8Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}
