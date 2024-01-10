use arrow2::array::{Array as ArrowArray, BooleanArray as ArrowBooleanArray};

use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;

use super::{Array, ArrayEncoding, ArrowIterator, IntoArrowIterator};

#[derive(Debug, Clone, PartialEq)]
pub struct BoolArray {
    buffer: ArrowBooleanArray,
}

impl BoolArray {
    pub fn new(buffer: ArrowBooleanArray) -> Self {
        Self { buffer }
    }
}

impl ArrayEncoding for BoolArray {
    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &DType::Bool
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        // TODO(ngates): this panics when index OOB
        Ok(arrow2::scalar::new_scalar(&self.buffer, index)
            .as_ref()
            .into())
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(std::iter::once(self.buffer.clone().boxed()))
    }

    fn into_iter_arrow(self) -> Box<IntoArrowIterator> {
        Box::new(std::iter::once(self.buffer.boxed()))
    }

    fn slice(&self, offset: usize, length: usize) -> Array {
        let mut cloned = self.clone();
        cloned.buffer.slice(offset, length);
        Array::Bool(cloned)
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Array {
        let mut cloned = self.clone();
        cloned.buffer.slice_unchecked(offset, length);
        Array::Bool(cloned)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn slice() {
        let arr = BoolArray::new(ArrowBooleanArray::from_slice([
            true, true, false, false, true,
        ]))
        .slice(1, 3);
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(true));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(false));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(false));
    }
}
