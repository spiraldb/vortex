use std::iter;

use arrow::array::{Array as ArrowArray, ArrayRef, Scalar as ArrowScalar};
use arrow::datatypes::DataType;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone)]
pub struct VarBinArray {
    buffer: ArrayRef,
    dtype: DType,
}

impl VarBinArray {
    pub fn new(buffer: ArrayRef) -> Self {
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
            Ok(ArrowScalar::new(self.buffer.slice(index, 1)).into())
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(iter::once(self.buffer.clone()))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<Array> {
        self.check_slice_bounds(start, stop)?;

        Ok(Array::VarBin(Self {
            buffer: self.buffer.slice(start, stop - start),
            dtype: self.dtype.clone(),
        }))
    }
}
