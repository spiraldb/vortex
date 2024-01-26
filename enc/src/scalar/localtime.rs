use crate::error::EncResult;
use crate::scalar::{PScalar, Scalar};
use crate::types::{DType, TimeUnit};
use std::any::Any;
use std::cmp::Ordering;

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

    fn nbytes(&self) -> usize {
        self.value.nbytes()
    }
}

impl PartialOrd for LocalTimeScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.dtype != other.dtype {
            None
        } else {
            self.value.partial_cmp(&other.value)
        }
    }
}
