use arrow2::array::Array as ArrowArray;
use arrow2::array::BinaryArray;
use arrow2::offset::Offset;
use std::iter;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct VarBinArray {
    buffer: Box<dyn ArrowArray>,
}

impl VarBinArray {
    pub fn new<O: Offset>(buffer: Box<BinaryArray<O>>) -> Self {
        Self { buffer }
    }
}

impl ArrayEncoding for VarBinArray {
    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn dtype(&self) -> DType {
        DType::Binary
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
        Ok(Array::VarBin(cloned))
    }
}
