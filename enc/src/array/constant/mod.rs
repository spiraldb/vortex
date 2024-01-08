use crate::array::{ArrayEncoding, ArrowIterator};
use crate::arrow;
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct ConstantArray {
    scalar: Box<dyn Scalar>,
    length: usize,
}

impl ConstantArray {
    pub fn new(scalar: Box<dyn Scalar>, length: usize) -> Self {
        Self { scalar, length }
    }

    pub fn value(&self) -> &dyn Scalar {
        self.scalar.as_ref()
    }
}

impl ArrayEncoding for ConstantArray {
    #[inline]
    fn len(&self) -> usize {
        self.length
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.scalar.dtype()
    }

    // TODO(robert): Return Result
    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        if index >= self.length {
            return Err(EncError::OutOfBounds(index, 0, self.length));
        }
        Ok(self.scalar.clone())
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let arrow_scalar: Box<dyn arrow2::scalar::Scalar> = self.scalar.as_ref().into();
        Box::new(std::iter::once(arrow::compute::repeat(
            arrow_scalar.as_ref(),
            self.length,
        )))
    }
}
