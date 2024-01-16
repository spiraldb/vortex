use arrow2::array::{Array as ArrowArray, BooleanArray as ArrowBooleanArray};

use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

use super::{Array, ArrayEncoding, ArrowIterator};

#[derive(Debug, Clone, PartialEq)]
pub struct BoolArray {
    buffer: Box<ArrowBooleanArray>,
}

impl BoolArray {
    pub fn new(buffer: Box<ArrowBooleanArray>) -> Self {
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
    fn dtype(&self) -> DType {
        DType::Bool
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        if index >= self.len() {
            Err(EncError::OutOfBounds(index, 0, self.len()))
        } else {
            Ok(arrow2::scalar::new_scalar(self.buffer.as_ref(), index).into())
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(std::iter::once(self.buffer.clone().boxed()))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<Array> {
        self.check_slice_bounds(start, stop)?;

        let mut cloned = self.clone();
        unsafe {
            cloned.buffer.slice_unchecked(start, stop - start);
        }
        Ok(Array::Bool(cloned))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn slice() {
        let arr = BoolArray::new(Box::new(ArrowBooleanArray::from_slice([
            true, true, false, false, true,
        ])))
        .slice(1, 4)
        .unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(true));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(false));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(false));
    }
}
