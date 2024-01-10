use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct Utf8Scalar {
    value: String,
}

impl Utf8Scalar {
    pub fn new(value: String) -> Self {
        Self { value }
    }

    pub fn value(&self) -> &String {
        &self.value
    }
}

impl Scalar for Utf8Scalar {
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
        DType::Utf8
    }
}
