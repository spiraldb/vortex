use crate::error::EncResult;
use crate::scalar::{PScalar, Scalar};
use crate::types::{DType, TimeUnit};

#[derive(Debug, Clone, PartialEq)]
pub struct LocalTimeScalar {
    value: PScalar,
    unit: TimeUnit,
}

impl LocalTimeScalar {
    pub fn new(value: PScalar, unit: TimeUnit) -> Self {
        Self { value, unit }
    }
}

impl Scalar for LocalTimeScalar {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn boxed(self) -> Box<dyn Scalar> {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> DType {
        DType::LocalTime(self.unit.clone())
    }

    fn cast(&self, _dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        todo!()
    }
}
