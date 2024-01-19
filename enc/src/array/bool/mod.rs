use std::iter;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::buffer::BooleanBuffer;

use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

use super::{Array, ArrayEncoding, ArrowIterator};

#[derive(Debug, Clone, PartialEq)]
pub struct BoolArray {
    buffer: BooleanBuffer,
}

impl BoolArray {
    pub fn new(buffer: BooleanBuffer) -> Self {
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
        if index >= self.len() {
            return Err(EncError::OutOfBounds(index, 0, self.len()));
        }

        if self.buffer.value(index) {
            Ok(true.into())
        } else {
            Ok(false.into())
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(iter::once(
            Arc::new(BooleanArray::from(self.buffer.clone())) as ArrayRef,
        ))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<Array> {
        self.check_slice_bounds(start, stop)?;

        Ok(Array::Bool(Self {
            buffer: self.buffer.slice(start, stop - start),
        }))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn slice() {
        let arr = BoolArray::new(BooleanBuffer::from(vec![true, true, false, false, true]))
            .slice(1, 4)
            .unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(true));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(false));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(false));
    }
}
