use std::fmt::{Display, Formatter};

use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::Scalar;

#[derive(Debug, Clone, PartialEq)]
pub struct CompositeScalar {
    dtype: DType,
    scalar: Box<Scalar>,
}

impl CompositeScalar {
    pub fn new(dtype: DType, scalar: Box<Scalar>) -> Self {
        Self { dtype, scalar }
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn scalar(&self) -> &Scalar {
        self.scalar.as_ref()
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
        self.scalar.nbytes()
    }
}

impl PartialOrd for CompositeScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.scalar.as_ref().partial_cmp(other.scalar.as_ref())
    }
}

impl Display for CompositeScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.scalar, self.dtype)
    }
}
