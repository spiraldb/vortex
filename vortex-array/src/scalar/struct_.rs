use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use itertools::Itertools;

use crate::dtype::DType;
use crate::error::{VortexError, VortexResult};
use crate::scalar::{Scalar, ScalarRef};

#[derive(Debug, Clone, PartialEq)]
pub struct StructScalar {
    dtype: DType,
    values: Vec<ScalarRef>,
}

impl StructScalar {
    #[inline]
    pub fn new(dtype: DType, values: Vec<ScalarRef>) -> Self {
        Self { dtype, values }
    }

    #[inline]
    pub fn values(&self) -> &[ScalarRef] {
        &self.values
    }
}

impl Scalar for StructScalar {
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
            DType::Struct(names, field_dtypes) => {
                if field_dtypes.len() != self.values.len() {
                    return Err(VortexError::InvalidDType(dtype.clone()));
                }

                let new_fields: Vec<ScalarRef> = self
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
            _ => Err(VortexError::InvalidDType(dtype.clone())),
        }
    }

    fn nbytes(&self) -> usize {
        self.values.iter().map(|s| s.nbytes()).sum()
    }
}

impl PartialOrd for StructScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.dtype != other.dtype {
            None
        } else {
            self.values.partial_cmp(&other.values)
        }
    }
}

impl Display for StructScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let DType::Struct(names, _) = self.dtype() else {
            unreachable!()
        };
        for (n, v) in names.iter().zip(self.values.iter()) {
            write!(f, "{} = {}", n, v)?;
        }
        Ok(())
    }
}
