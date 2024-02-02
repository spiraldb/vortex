use std::any::Any;
use std::iter;
use std::sync::{Arc, RwLock};

use arrow::array::{make_array, Array as ArrowArray, ArrayData, AsArray};
use arrow::datatypes::UInt8Type;

use crate::array::formatter::{ArrayDisplay, ArrayFormatter};
use crate::array::stats::{Stats, StatsSet};
use crate::array::{
    check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef,
};
use crate::arrow::CombineChunks;
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::{DType, IntWidth, Nullability, Signedness};

#[derive(Debug, Clone)]
pub struct VarBinArray {
    offsets: ArrayRef,
    bytes: ArrayRef,
    dtype: DType,
    validity: Option<ArrayRef>,
    stats: Arc<RwLock<StatsSet>>,
}

impl VarBinArray {
    pub fn new(offsets: ArrayRef, bytes: ArrayRef, dtype: DType) -> Self {
        if !matches!(offsets.dtype(), DType::Int(_, _, Nullability::NonNullable)) {
            panic!("Unsupported type for offsets array");
        }
        if !matches!(
            bytes.dtype(),
            DType::Int(IntWidth::_8, Signedness::Unsigned, _)
        ) {
            panic!("Unsupported type for data array {:?}", bytes.dtype());
        }
        if !matches!(dtype, DType::Binary(_) | DType::Utf8(_)) {
            panic!("Unsupported dtype for varbin array");
        }
        Self {
            offsets,
            bytes,
            dtype,
            validity: None,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    #[inline]
    pub fn offsets(&self) -> &dyn Array {
        self.offsets.as_ref()
    }

    #[inline]
    pub fn bytes(&self) -> &dyn Array {
        self.bytes.as_ref()
    }

    #[inline]
    pub fn validity(&self) -> Option<&ArrayRef> {
        self.validity.as_ref()
    }

    pub fn bytes_at(&self, index: usize) -> EncResult<Vec<u8>> {
        if index > self.len() {
            return Err(EncError::OutOfBounds(index, 0, self.len()));
        }

        let offset_start: usize = self.offsets().scalar_at(index)?.try_into()?;
        let offset_end: usize = self.offsets().scalar_at(index + 1)?.try_into()?;
        let sliced = self.bytes().slice(offset_start, offset_end)?;
        let arr_ref = sliced.iter_arrow().combine_chunks();
        Ok(arr_ref.as_primitive::<UInt8Type>().values().to_vec())
    }
}

impl Array for VarBinArray {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

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
            if matches!(self.dtype, DType::Utf8(_)) {
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

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(VarBinArray::new(
            self.offsets.slice(start, stop + 1)?,
            self.bytes.clone(),
            self.dtype.clone(),
        )
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &VarBinEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.bytes.nbytes() + self.offsets.nbytes()
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for VarBinArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

#[derive(Debug)]
struct VarBinEncoding;

pub const VARBIN_ENCODING: EncodingId = EncodingId("enc.varbin");

impl Encoding for VarBinEncoding {
    fn id(&self) -> &EncodingId {
        &VARBIN_ENCODING
    }
}

impl ArrayDisplay for VarBinArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("offsets:")?;
        f.indent(|ind| ind.array(self.offsets()))?;
        f.writeln("bytes:")?;
        f.indent(|ind| ind.array(self.bytes()))
    }
}

#[cfg(test)]
mod test {
    use arrow::array::GenericStringArray as ArrowStringArray;

    use crate::array::primitive::PrimitiveArray;

    use super::*;

    fn binary_array() -> VarBinArray {
        let values = PrimitiveArray::from_vec(
            "hello worldhello world this is a long string"
                .as_bytes()
                .to_vec(),
        );
        let offsets = PrimitiveArray::from_vec(vec![0, 11, 44]);

        VarBinArray::new(
            offsets.boxed(),
            values.boxed(),
            DType::Utf8(Nullability::NonNullable),
        )
    }

    #[test]
    pub fn scalar_at() {
        let binary_arr = binary_array();
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(binary_arr.scalar_at(0), Ok("hello world".into()));
        assert_eq!(
            binary_arr.scalar_at(1),
            Ok("hello world this is a long string".into())
        )
    }

    #[test]
    pub fn slice() {
        let binary_arr = binary_array().slice(1, 2).unwrap();
        assert_eq!(
            binary_arr.scalar_at(0),
            Ok("hello world this is a long string".into())
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
                "hello world this is a long string",
            ])
        );
    }
}
