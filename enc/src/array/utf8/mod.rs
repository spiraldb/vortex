use std::iter;

use arrow2::array::{Array as ArrowArray, Utf8Array as ArrowUtf8Array};
use arrow2::offset::Offset;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct Utf8Array {
    buffer: Box<dyn ArrowArray>,
}

impl Utf8Array {
    pub fn new<O: Offset>(buffer: Box<ArrowUtf8Array<O>>) -> Self {
        Self { buffer }
    }
}

impl ArrayEncoding for Utf8Array {
    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn dtype(&self) -> DType {
        DType::Utf8
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        if index >= self.len() {
            Err(EncError::OutOfBounds(index, 0, self.len()))
        } else {
            Ok(arrow2::scalar::new_scalar(self.buffer.as_ref(), index).into())
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(iter::once(self.buffer.clone()))
    }

    fn slice(&self, offset: usize, length: usize) -> EncResult<Array> {
        self.check_slice_bounds(offset, length)?;

        let mut cloned = self.clone();
        unsafe {
            cloned.buffer.slice_unchecked(offset, length);
        }
        Ok(Array::Utf8(cloned))
    }
}
