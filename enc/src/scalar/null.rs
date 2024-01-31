use std::any::Any;
use std::fmt::{Display, Formatter};

use crate::error::EncResult;
use crate::scalar::{NullableScalar, Scalar};
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct NullScalar;

impl Default for NullScalar {
    fn default() -> Self {
        Self::new()
    }
}

impl NullScalar {
    #[inline]
    pub fn new() -> Self {
        Self {}
    }
}

impl Scalar for NullScalar {
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
        &DType::Null
    }

    fn cast(&self, dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        Ok(NullableScalar::none(dtype.clone()).boxed())
    }

    fn nbytes(&self) -> usize {
        1
    }
}

impl Display for NullScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "null")
    }
}
