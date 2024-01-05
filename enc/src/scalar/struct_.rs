use std::any::Any;

use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct StructScalar {
    values: Vec<Box<dyn Scalar>>,
    dtype: DType,
}

impl StructScalar {
    #[inline]
    pub fn new(names: Vec<String>, values: Vec<Box<dyn Scalar>>) -> Self {
        let dtypes = values.iter().map(|x| x.dtype().clone()).collect();
        Self {
            values,
            dtype: DType::Struct(names, dtypes),
        }
    }

    #[inline]
    pub fn values(&self) -> &[Box<dyn Scalar>] {
        &self.values
    }
}

impl Scalar for StructScalar {
    #[inline]
    fn as_any(&self) -> &dyn Any {
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
}
