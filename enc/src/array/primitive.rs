use core::cmp::min;
use std::any::Any;
use std::iter;
use std::mem::size_of;
use std::panic::RefUnwindSafe;
use std::ptr::NonNull;
use std::sync::{Arc, RwLock};

use allocator_api2::alloc::Allocator;
use arrow::array::{make_array, ArrayData};
use arrow::buffer::Buffer;
use half::f16;

use crate::array::formatter::{ArrayDisplay, ArrayFormatter};
use crate::array::stats::{Stats, StatsSet};
use crate::array::{
    check_index_bounds, check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId,
    EncodingRef,
};
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::{match_each_native_ptype, DType, NativePType, PType};

#[derive(Debug, Clone)]
pub struct PrimitiveArray {
    buffer: Buffer,
    ptype: PType,
    dtype: DType,
    validity: Option<ArrayRef>,
    stats: Arc<RwLock<StatsSet>>,
}

impl PrimitiveArray {
    pub fn new(ptype: PType, buffer: Buffer) -> Self {
        let dtype: DType = ptype.into();
        Self {
            buffer,
            ptype,
            dtype,
            validity: None,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    pub fn from_vec<T: NativePType>(values: Vec<T>) -> Self {
        let buffer = Buffer::from_vec::<T>(values);
        Self::new(T::PTYPE, buffer)
    }

    /// Allocate buffer from allocator-api2 vector. This would be easier when arrow gets https://github.com/apache/arrow-rs/issues/3960
    pub fn from_vec_in<T: NativePType, A: Allocator + RefUnwindSafe + Send + Sync + 'static>(
        values: allocator_api2::vec::Vec<T, A>,
    ) -> Self {
        let ptr = values.as_ptr();
        let buffer = unsafe {
            Buffer::from_custom_allocation(
                NonNull::new(ptr as _).unwrap(),
                values.len() * size_of::<T>(),
                Arc::new(values),
            )
        };
        Self::new(T::PTYPE, buffer)
    }

    #[inline]
    pub fn ptype(&self) -> &PType {
        &self.ptype
    }

    #[inline]
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    #[inline]
    pub fn validity(&self) -> Option<&ArrayRef> {
        self.validity.as_ref()
    }
}

impl Array for PrimitiveArray {
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
        self.buffer.len() / self.ptype.byte_width()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
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
        check_index_bounds(self, index)?;

        Ok(
            match_each_native_ptype!(self.ptype, |$T| self.buffer.typed_data::<$T>()
                .get(index)
                .unwrap()
                .clone()
                .into()
            ),
        )
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(iter::once(make_array(
            ArrayData::builder(self.dtype().into())
                .len(self.len())
                .nulls(None)
                .add_buffer(self.buffer.clone())
                .build()
                .unwrap(),
        )))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        let byte_start = start * self.ptype.byte_width();
        let byte_length = (stop - start) * self.ptype.byte_width();

        Ok(Self {
            buffer: self.buffer.slice_with_length(byte_start, byte_length),
            ptype: self.ptype,
            validity: self.validity.clone(),
            dtype: self.dtype.clone(),
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &PrimitiveEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.buffer.len()
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for PrimitiveArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

#[derive(Debug)]
pub struct PrimitiveEncoding;

pub const PRIMITIVE_ENCODING: EncodingId = EncodingId("enc.primitive");

impl Encoding for PrimitiveEncoding {
    fn id(&self) -> &EncodingId {
        &PRIMITIVE_ENCODING
    }
}

impl<T: NativePType> From<Vec<T>> for ArrayRef {
    fn from(values: Vec<T>) -> Self {
        PrimitiveArray::from_vec(values).boxed()
    }
}

impl ArrayDisplay for PrimitiveArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        match_each_native_ptype!(self.ptype(), |$P| {
            f.writeln(format!("{:?}{}",
                &self.buffer().typed_data::<$P>()[..min(10, self.len())],
                if self.len() > 10 { "..." } else { "" }))
        })
    }
}

#[cfg(test)]
mod test {
    use crate::types::{IntWidth, Nullability, Signedness};

    use super::*;

    #[test]
    fn from_arrow() {
        let arr = PrimitiveArray::from_vec::<i32>(vec![1, 2, 3]);
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.ptype, PType::I32);
        assert_eq!(
            arr.dtype(),
            &DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable)
        );

        // Ensure we can fetch the scalar at the given index.
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(1));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(3));
    }

    #[test]
    fn slice() {
        let arr = PrimitiveArray::from_vec(vec![1, 2, 3, 4, 5])
            .slice(1, 4)
            .unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(3));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(4));
    }
}
