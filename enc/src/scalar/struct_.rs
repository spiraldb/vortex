use std::any::Any;

use itertools::Itertools;

use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct StructScalar {
    dtype: DType,
    values: Vec<Box<dyn Scalar>>,
}

impl StructScalar {
    #[inline]
    pub fn new(dtype: DType, values: Vec<Box<dyn Scalar>>) -> Self {
        Self { dtype, values }
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

    fn cast(&self, dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        match dtype {
            DType::Struct(names, field_dtypes) => {
                if field_dtypes.len() != self.values.len() {
                    return Err(EncError::InvalidDType(dtype.clone()));
                }

                let new_fields: Vec<Box<dyn Scalar>> = self
                    .values
                    .iter()
                    .zip_eq(field_dtypes.iter())
                    .map(|(field, field_dtype)| field.cast(field_dtype))
                    .try_collect()?;

                let new_type = DType::Struct(
                    names.clone(),
                    new_fields.iter().map(|x| x.dtype().clone()).collect(),
                );
                Ok(StructScalar::new(new_type, new_fields).boxed())
            }
            _ => Err(EncError::InvalidDType(dtype.clone())),
        }
    }
}
