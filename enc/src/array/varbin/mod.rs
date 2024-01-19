use std::iter;

use arrow::array::{make_array, Array as ArrowArray, ArrayData};

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::arrow::CombineChunks;
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::{DType, IntWidth};

#[derive(Debug, Clone)]
pub struct VarBinArray {
    offsets: Box<Array>,
    bytes: Box<Array>,
    dtype: DType,
}

impl VarBinArray {
    pub fn new(offsets: Box<Array>, bytes: Box<Array>, dtype: DType) -> Self {
        if !matches!(offsets.dtype(), DType::UInt(_) | DType::Int(_)) {
            panic!("Unsupported type for offsets array");
        }
        if !matches!(bytes.dtype(), DType::UInt(IntWidth::_8)) {
            panic!("Unsupported type for data array {:?}", bytes.dtype());
        }
        if !matches!(dtype, DType::Binary | DType::Utf8) {
            panic!("Unsupported dtype for varbin array");
        }
        Self {
            offsets,
            bytes,
            dtype,
        }
    }
}

impl ArrayEncoding for VarBinArray {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.offsets.len() <= 1
    }

    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn scalar_at(&self, _index: usize) -> EncResult<Box<dyn Scalar>> {
        todo!("Implement scalar_at for VarBinArray using searchsorted");
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let offsets_data = self.offsets.iter_arrow().combine_chunks().into_data();
        let bytes_data = self.bytes.iter_arrow().combine_chunks().into_data();

        let data = ArrayData::builder(self.dtype.clone().into())
            .len(self.len())
            .nulls(None)
            .add_buffer(offsets_data.buffers()[0].to_owned())
            .add_buffer(bytes_data.buffers()[0].to_owned())
            .build()
            .unwrap();

        Box::new(iter::once(make_array(data)))
    }

    fn slice(&self, _start: usize, _stop: usize) -> EncResult<Array> {
        todo!()
    }
}
