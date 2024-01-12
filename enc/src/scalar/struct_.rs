use itertools::Itertools;
use std::any::Any;

use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct StructScalar {
    names: Vec<String>,
    values: Vec<Box<dyn Scalar>>,
}

impl StructScalar {
    #[inline]
    pub fn new(names: Vec<String>, values: Vec<Box<dyn Scalar>>) -> Self {
        Self { names, values }
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
    fn dtype(&self) -> DType {
        DType::Struct(
            self.names.clone(),
            self.values.iter().map(|x| x.dtype().clone()).collect(),
        )
    }

    fn cast(&self, dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        match dtype {
            DType::Struct(names, field_dtypes) => {
                if field_dtypes.len() != self.values.len() {
                    return Err(EncError::InvalidDType(dtype.clone()));
                }

                let new_fields = self
                    .values
                    .iter()
                    .zip_eq(field_dtypes.iter())
                    .map(|(field, field_dtype)| field.cast(field_dtype))
                    .try_collect()?;

                Ok(StructScalar::new(names.clone(), new_fields).boxed())
            }
            _ => Err(EncError::InvalidDType(dtype.clone())),
        }
    }
}
