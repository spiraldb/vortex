use std::any::Any;

use crate::error;
use itertools::Itertools;

use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct ListScalar {
    dtype: DType,
    values: Vec<Box<dyn Scalar>>,
}

impl ListScalar {
    #[inline]
    pub fn new(dtype: DType, values: Vec<Box<dyn Scalar>>) -> Self {
        Self { dtype, values }
    }

    #[inline]
    pub fn values(&self) -> &[Box<dyn Scalar>] {
        &self.values
    }
}

impl Scalar for ListScalar {
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

    fn cast(&self, dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        match dtype {
            DType::List(field_dtype) => {
                let new_fields: Vec<Box<dyn Scalar>> = self
                    .values
                    .iter()
                    .map(|field| field.cast(field_dtype))
                    .try_collect()?;

                let new_type = if new_fields.is_empty() {
                    dtype.clone()
                } else {
                    DType::List(Box::new(new_fields[0].dtype().clone()))
                };
                Ok(ListScalar::new(new_type, new_fields).boxed())
            }
            _ => Err(EncError::InvalidDType(dtype.clone())),
        }
    }
}

impl<I: Into<Box<dyn Scalar>>, T: IntoIterator<Item = I>> From<T> for ListScalar {
    fn from(value: T) -> Self {
        let values: Vec<Box<dyn Scalar>> = value.into_iter().map(|v| v.into()).collect();
        if values.is_empty() {
            panic!("Can't implicitly convert empty list into ListScalar");
        }
        ListScalar::new(values[0].dtype().clone(), values)
    }
}

impl<T: TryFrom<Box<dyn Scalar>, Error = error::EncError>> TryFrom<Box<dyn Scalar>> for Vec<T> {
    type Error = EncError;

    fn try_from(value: Box<dyn Scalar>) -> Result<Self, Self::Error> {
        let value_dtype = value.dtype().clone();
        value
            .into_any()
            .downcast::<ListScalar>()
            .map_err(|_| EncError::InvalidDType(value_dtype))
            .and_then(|list_s| {
                list_s
                    .values
                    .clone()
                    .into_iter()
                    .map(|v| v.try_into())
                    .try_collect()
            })
    }
}
