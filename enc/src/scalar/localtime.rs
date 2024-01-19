use crate::error::EncResult;
use crate::scalar::{PScalar, Scalar};
use crate::types::{DType, TimeUnit};
use std::any::Any;

#[derive(Debug, Clone, PartialEq)]
pub struct LocalTimeScalar {
    value: PScalar,
    dtype: DType,
}

impl LocalTimeScalar {
    pub fn new(value: PScalar, unit: TimeUnit) -> Self {
        Self {
            value,
            dtype: DType::LocalTime(unit),
        }
    }
}

impl Scalar for LocalTimeScalar {
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
        &self.dtype
    }

    fn cast(&self, _dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        todo!()
    }
}
