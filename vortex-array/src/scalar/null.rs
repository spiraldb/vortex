use std::any::Any;
use std::fmt::{Display, Formatter};

use crate::dtype::DType;
use crate::error::VortexResult;
use crate::scalar::{NullableScalar, Scalar, ScalarRef};

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
    fn as_nonnull(&self) -> Option<&dyn Scalar> {
        None
    }

    #[inline]
    fn into_nonnull(self: Box<Self>) -> Option<ScalarRef> {
        None
    }

    #[inline]
    fn boxed(self) -> ScalarRef {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &DType::Null
    }

    fn cast(&self, dtype: &DType) -> VortexResult<ScalarRef> {
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
