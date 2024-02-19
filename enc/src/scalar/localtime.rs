use crate::dtype::{DType, Nullability, TimeUnit};
use crate::error::EncResult;
use crate::scalar::{PScalar, Scalar};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct LocalTimeScalar {
    value: PScalar,
    dtype: DType,
}

impl LocalTimeScalar {
    pub fn new(value: PScalar, unit: TimeUnit) -> Self {
        Self {
            value,
            dtype: DType::LocalTime(unit, Nullability::NonNullable),
        }
    }
}

impl Scalar for LocalTimeScalar {
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
    fn into_nonnull(self: Box<Self>) -> Option<Box<dyn Scalar>> {
        Some(self)
    }

    #[inline]
    fn boxed(self) -> Box<dyn Scalar> {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn cast(&self, _dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        todo!()
    }

    fn nbytes(&self) -> usize {
        self.value.nbytes()
    }
}

impl PartialOrd for LocalTimeScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.dtype() != other.dtype() {
            None
        } else {
            self.value.partial_cmp(&other.value)
        }
    }
}

impl Display for LocalTimeScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let DType::LocalTime(u, _) = self.dtype() else {
            unreachable!()
        };
        write!(f, "localtime[{}, unit={}]", self.value, u)
    }
}
