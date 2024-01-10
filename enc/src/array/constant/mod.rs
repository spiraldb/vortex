use crate::array::{Array, ArrayEncoding, ArrowIterator, IntoArrowIterator};
use crate::arrow;
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct ConstantArray {
    scalar: Box<dyn Scalar>,
    length: usize,
    offset: usize,
}

impl ConstantArray {
    pub fn new(scalar: Box<dyn Scalar>, length: usize) -> Self {
        Self {
            scalar,
            length,
            offset: 0,
        }
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

    fn into_iter_arrow(self) -> Box<IntoArrowIterator> {
        let arrow_scalar: Box<dyn arrow2::scalar::Scalar> = self.scalar.as_ref().into();
        Box::new(std::iter::once(arrow::compute::repeat(
            arrow_scalar.as_ref(),
            self.length,
        )))
    }

    fn slice(&self, offset: usize, length: usize) -> Array {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Array {
        let mut cloned = self.clone();
        cloned.offset += offset;
        cloned.length = length;
        Array::Constant(cloned)
    }
}
