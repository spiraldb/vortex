use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use crate::dtype::{DType, Nullability, TimeUnit};
use crate::error::VortexResult;
use crate::scalar::{PrimitiveScalar, Scalar};

#[derive(Debug, Clone, PartialEq)]
pub struct LocalTimeScalar {
    value: PrimitiveScalar,
    dtype: DType,
}

impl LocalTimeScalar {
    pub fn new(value: PrimitiveScalar, unit: TimeUnit) -> Self {
        Self {
            value,
            dtype: DType::LocalTime(unit, Nullability::NonNullable),
        }
    }

    pub fn value(&self) -> &PrimitiveScalar {
        &self.value
    }

    pub fn time_unit(&self) -> TimeUnit {
        let DType::LocalTime(u, _) = self.dtype else {
            unreachable!("unexpected dtype")
        };
        u
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
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
