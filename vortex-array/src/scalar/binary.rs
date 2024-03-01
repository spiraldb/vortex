use crate::dtype::{DType, Nullability};
use crate::error::{VortexError, VortexResult};
use crate::scalar::{Scalar, ScalarRef};
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
        &DType::Binary(Nullability::NonNullable)
    }

    fn cast(&self, _dtype: &DType) -> VortexResult<ScalarRef> {
        todo!()
    }

    fn nbytes(&self) -> usize {
        self.value.len()
    }
}

impl From<Vec<u8>> for ScalarRef {
    fn from(value: Vec<u8>) -> Self {
        BinaryScalar::new(value).boxed()
    }
}

impl TryFrom<ScalarRef> for Vec<u8> {
    type Error = VortexError;

    fn try_from(value: ScalarRef) -> Result<Self, Self::Error> {
        let dtype = value.dtype().clone();
        let scalar = value
            .into_any()
            .downcast::<BinaryScalar>()
            .map_err(|_| VortexError::InvalidDType(dtype))?;
        Ok(scalar.value)
    }
}

impl TryFrom<&dyn Scalar> for Vec<u8> {
    type Error = VortexError;

    fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
        if let Some(scalar) = value.as_any().downcast_ref::<BinaryScalar>() {
            Ok(scalar.value.clone())
        } else {
            Err(VortexError::InvalidDType(value.dtype().clone()))
        }
    }
}

impl Display for BinaryScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "bytes[{}]", self.value.len())
    }
}
