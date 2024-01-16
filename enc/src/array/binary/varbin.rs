use arrow2::array::Array as ArrowArray;

use arrow2::datatypes::DataType;

use std::iter;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct VarBinArray {
    buffer: Box<dyn ArrowArray>,
    dtype: DType,
}

impl VarBinArray {
    pub fn new(buffer: Box<dyn ArrowArray>) -> Self {
        let dtype = match buffer.data_type() {
            DataType::Binary => DType::Binary,
            DataType::LargeBinary => DType::Binary,
            DataType::Utf8 => DType::Utf8,
            DataType::LargeUtf8 => DType::Utf8,
            _ => panic!("Unsupported array type"),
        };
        Self { buffer, dtype }
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
        self.dtype.clone()
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

    fn slice(&self, start: usize, stop: usize) -> EncResult<Array> {
        self.check_slice_bounds(start, stop)?;

        let mut cloned = self.clone();
        unsafe {
            cloned.buffer.slice_unchecked(start, stop - start);
        }
        Ok(Array::VarBin(cloned))
    }
}
