use std::any::Any;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

use crate::dtype::{DType, Nullability};
use crate::error::{VortexError, VortexResult};
use crate::scalar::{NullableScalar, Scalar, ScalarRef};

#[derive(Debug, Clone, PartialEq)]
pub struct ListScalar {
    dtype: DType,
    values: Vec<ScalarRef>,
}

impl ListScalar {
    #[inline]
    pub fn new(dtype: DType, values: Vec<ScalarRef>) -> Self {
        Self { dtype, values }
    }

    #[inline]
    pub fn values(&self) -> &[ScalarRef] {
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
    fn as_nonnull(&self) -> Option<&dyn Scalar> {
        Some(self)
    }

    #[inline]
    fn into_nonnull(self: Box<Self>) -> Option<ScalarRef> {
        Some(self)
    }

    #[inline]
    fn boxed(self) -> ScalarRef {
        Box::new(self)
    }
    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn cast(&self, dtype: &DType) -> VortexResult<ScalarRef> {
        match dtype {
            DType::List(field_dtype, n) => {
                let new_fields: Vec<ScalarRef> = self
                    .values
                    .iter()
                    .map(|field| field.cast(field_dtype))
                    .try_collect()?;

                let new_type = if new_fields.is_empty() {
                    dtype.clone()
                } else {
                    DType::List(Box::new(new_fields[0].dtype().clone()), *n)
                };
                let list_scalar = ListScalar::new(new_type, new_fields).boxed();
                match n {
                    Nullability::NonNullable => Ok(list_scalar),
                    Nullability::Nullable => Ok(NullableScalar::some(list_scalar).boxed()),
                }
            }
            _ => Err(VortexError::InvalidDType(dtype.clone())),
        }
    }

    fn nbytes(&self) -> usize {
        self.values.iter().map(|s| s.nbytes()).sum()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListScalarVec<T>(pub Vec<T>);

impl<T: Into<ScalarRef>> From<ListScalarVec<T>> for ScalarRef {
    fn from(value: ListScalarVec<T>) -> Self {
        let values: Vec<ScalarRef> = value.0.into_iter().map(|v| v.into()).collect();
        if values.is_empty() {
            panic!("Can't implicitly convert empty list into ListScalar");
        }
        ListScalar::new(values[0].dtype().clone(), values).boxed()
    }
}

impl<T: TryFrom<ScalarRef, Error = VortexError>> TryFrom<&dyn Scalar> for ListScalarVec<T> {
    type Error = VortexError;

    fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
        if let Some(list_s) = value.as_any().downcast_ref::<ListScalar>() {
            Ok(ListScalarVec(
                list_s
                    .values
                    .clone()
                    .into_iter()
                    .map(|v| v.try_into())
                    .try_collect()?,
            ))
        } else {
            Err(VortexError::InvalidDType(value.dtype().clone()))
        }
    }
}

impl<T: TryFrom<ScalarRef, Error = VortexError>> TryFrom<ScalarRef> for ListScalarVec<T> {
    type Error = VortexError;

    fn try_from(value: ScalarRef) -> Result<Self, Self::Error> {
        let value_dtype = value.dtype().clone();
        let list_s = value
            .into_any()
            .downcast::<ListScalar>()
            .map_err(|_| VortexError::InvalidDType(value_dtype))?;

        Ok(ListScalarVec(
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
