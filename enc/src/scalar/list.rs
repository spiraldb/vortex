use std::any::Any;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

use crate::dtype::{DType, Nullability};
use crate::error::{EncError, EncResult};
use crate::scalar::{NullableScalar, Scalar};

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
            DType::List(field_dtype, n) => {
                let new_fields: Vec<Box<dyn Scalar>> = self
                    .values
                    .iter()
                    .map(|field| field.cast(field_dtype))
                    .try_collect()?;

                let new_type = if new_fields.is_empty() {
                    dtype.clone()
                } else {
                    DType::List(Box::new(new_fields[0].dtype().clone()), n.clone())
                };
                let list_scalar = ListScalar::new(new_type, new_fields).boxed();
                match n {
                    Nullability::NonNullable => Ok(list_scalar),
                    Nullability::Nullable => Ok(NullableScalar::some(list_scalar).boxed()),
                }
            }
            _ => Err(EncError::InvalidDType(dtype.clone())),
        }
    }

    fn nbytes(&self) -> usize {
        self.values.iter().map(|s| s.nbytes()).sum()
    }
}

impl<T: Into<Box<dyn Scalar>>> From<ListScalarValues<T>> for Box<dyn Scalar> {
    fn from(value: ListScalarValues<T>) -> Self {
        let values: Vec<Box<dyn Scalar>> = value.0.into_iter().map(|v| v.into()).collect();
        if values.is_empty() {
            panic!("Can't implicitly convert empty list into ListScalar");
        }
        ListScalar::new(values[0].dtype().clone(), values).boxed()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListScalarValues<T>(pub Vec<T>);

impl<T: TryFrom<Box<dyn Scalar>, Error = EncError>> TryFrom<&dyn Scalar> for ListScalarValues<T> {
    type Error = EncError;

    fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
        if let Some(list_s) = value.as_any().downcast_ref::<ListScalar>() {
            Ok(ListScalarValues(
                list_s
                    .values
                    .clone()
                    .into_iter()
                    .map(|v| v.try_into())
                    .try_collect()?,
            ))
        } else {
            Err(EncError::InvalidDType(value.dtype().clone()))
        }
    }
}

impl<T: TryFrom<Box<dyn Scalar>, Error = EncError>> TryFrom<Box<dyn Scalar>>
    for ListScalarValues<T>
{
    type Error = EncError;

    fn try_from(value: Box<dyn Scalar>) -> Result<Self, Self::Error> {
        let value_dtype = value.dtype().clone();
        let list_s = value
            .into_any()
            .downcast::<ListScalar>()
            .map_err(|_| EncError::InvalidDType(value_dtype))?;

        Ok(ListScalarValues(
            list_s
                .values
                .into_iter()
                .map(|v| v.try_into())
                .try_collect()?,
        ))
    }
}

impl Display for ListScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.values.iter().format(", "))
    }
}
