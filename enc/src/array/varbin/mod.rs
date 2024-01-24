use std::iter;
use std::sync::{Arc, RwLock};

use arrow::array::{make_array, Array as ArrowArray, ArrayData, AsArray};
use arrow::datatypes::UInt8Type;

use crate::array::stats::{Stats, StatsSet};
pub use crate::array::varbin::stats::BinaryArray;
use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::arrow::CombineChunks;
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::{DType, IntWidth, Signedness};

mod stats;

#[derive(Debug, Clone)]
pub struct VarBinArray {
    offsets: Box<Array>,
    bytes: Box<Array>,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl VarBinArray {
    pub fn new(offsets: Box<Array>, bytes: Box<Array>, dtype: DType) -> Self {
        if !matches!(offsets.dtype(), DType::Int(_, _)) {
            panic!("Unsupported type for offsets array");
        }
        if !matches!(
            bytes.dtype(),
            DType::Int(IntWidth::_8, Signedness::Unsigned)
        ) {
            panic!("Unsupported type for data array {:?}", bytes.dtype());
        }
        if !matches!(dtype, DType::Binary | DType::Utf8) {
            panic!("Unsupported dtype for varbin array");
        }
        Self {
            offsets,
            bytes,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }
}

impl BinaryArray for VarBinArray {
    fn bytes_at(&self, index: usize) -> EncResult<Vec<u8>> {
        if index > self.len() {
            return Err(EncError::OutOfBounds(index, 0, self.len()));
        }

        let offset_start: usize = self.offsets.scalar_at(index)?.try_into()?;
        let offset_end: usize = self.offsets.scalar_at(index + 1)?.try_into()?;
        let sliced = self.bytes.slice(offset_start, offset_end)?;
        let arr_ref = sliced.iter_arrow().combine_chunks();
        Ok(arr_ref.as_primitive::<UInt8Type>().values().to_vec())
    }
}

impl ArrayEncoding for VarBinArray {
    #[inline]
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.offsets.len() <= 1
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        self.bytes_at(index).map(|bytes| {
            if matches!(self.dtype, DType::Utf8) {
                unsafe { String::from_utf8_unchecked(bytes) }.into()
            } else {
                bytes.into()
            }
        })
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

        let arr = make_array(data);
        Box::new(iter::once(arr))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<Array> {
        self.check_slice_bounds(start, stop)?;

        Ok(Array::VarBin(VarBinArray::new(
            Box::new(self.offsets.slice(start, stop + 1)?),
            self.bytes.clone(),
            self.dtype.clone(),
        )))
    }
}

#[cfg(test)]
mod test {
    use arrow::array::GenericStringArray as ArrowStringArray;

    use crate::array::primitive::PrimitiveArray;
    use crate::scalar::Utf8Scalar;

    use super::*;

    fn binary_array() -> VarBinArray {
        let values = PrimitiveArray::from_vec(
            "hello worldhello world this is a long string"
                .as_bytes()
                .to_vec(),
        );
        let offsets = PrimitiveArray::from_vec(vec![0, 11, 44]);

        VarBinArray::new(
            Box::new(offsets.into()),
            Box::new(values.into()),
            DType::Utf8,
        )
    }

    #[test]
    pub fn scalar_at() {
        let binary_arr = binary_array();
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(
            binary_arr.scalar_at(0).unwrap(),
            Utf8Scalar::new("hello world".into()).boxed()
        );
        assert_eq!(
            binary_arr.scalar_at(1).unwrap(),
            Utf8Scalar::new("hello world this is a long string".into()).boxed()
        )
    }

    #[test]
    pub fn slice() {
        let binary_arr = binary_array().slice(1, 2).unwrap();
        assert_eq!(
            binary_arr.scalar_at(0).unwrap(),
            Utf8Scalar::new("hello world this is a long string".into()).boxed()
        );
    }

    #[test]
    pub fn iter() {
        let binary_array = binary_array();
        assert_eq!(
            binary_array
                .iter_arrow()
                .combine_chunks()
                .as_string::<i32>(),
            &ArrowStringArray::<i32>::from(vec![
                "hello world",
                "hello world this is a long string"
            ])
        );
    }
}
