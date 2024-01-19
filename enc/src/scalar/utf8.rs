use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;
use std::any::Any;

#[derive(Debug, Clone, PartialEq)]
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
}
