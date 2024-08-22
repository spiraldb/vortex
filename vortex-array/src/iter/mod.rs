use core::slice;
use std::marker::PhantomData;
use std::sync::Arc;

pub use adapter::*;
pub use ext::*;
use vortex_dtype::{DType, NativePType};
use vortex_error::VortexResult;

use crate::array::PrimitiveArray;
use crate::validity::ArrayValidity;
use crate::Array;

mod adapter;
mod ext;

pub const DEFAULT_BATCH_SIZE: usize = 1024;

/// A stream of array chunks along with a DType.
/// Analogous to Arrow's RecordBatchReader.
pub trait ArrayIterator: Iterator<Item = VortexResult<Array>> {
    fn dtype(&self) -> &DType;
}

pub struct ArrayIter<T> {
    accessor: Arc<dyn Accessor<T>>,
    cached_batch: Option<Vec<T>>,
    batch_idx: usize,
    overall_idx: usize,
}

impl<T> ArrayIter<T> {
    pub fn new(accessor: Arc<dyn Accessor<T>>) -> Self {
        Self {
            accessor,
            cached_batch: None,
            batch_idx: 0,
            overall_idx: 0,
        }
    }
}

pub trait Accessor<O> {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn is_valid(&self, index: usize) -> bool;
    fn value_unchecked(&self, index: usize) -> O;
    fn decode_batch(&self, start_idx: usize, batch_size: Option<usize>) -> Vec<O> {
        let batch_size = batch_size
            .unwrap_or(DEFAULT_BATCH_SIZE)
            .min(self.len() - start_idx);
        let mut v = Vec::with_capacity(batch_size);
        // Safety:
        // We make sure above that we have at least `batch_size` elements to put into
        // the vector and sufficient capacity.
        unsafe {
            v.set_len(batch_size);
        }

        for idx in start_idx..start_idx + batch_size {
            v[idx] = self.value_unchecked(idx);
        }

        v
    }
}

pub struct PrimitiveAccessor<T> {
    array: PrimitiveArray,
    _marker: PhantomData<T>,
}

impl<T: NativePType> Accessor<T> for PrimitiveAccessor<T> {
    fn len(&self) -> usize {
        self.array.len()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.array.is_valid(index)
    }

    fn value_unchecked(&self, index: usize) -> T {
        let start = index * std::mem::size_of::<T>();
        let end = (index + 1) * std::mem::size_of::<T>();
        T::try_from_le_bytes(&self.array.buffer()[start..end]).unwrap()
    }

    fn decode_batch(&self, start_idx: usize, batch_size: Option<usize>) -> Vec<T> {
        let batch_size = batch_size
            .unwrap_or(DEFAULT_BATCH_SIZE)
            .min(self.len() - start_idx);

        let start = start_idx * std::mem::size_of::<T>();
        let end = (start_idx + batch_size) * std::mem::size_of::<T>();

        let bytes = &self.array.buffer()[start..end];

        let items = unsafe { slice::from_raw_parts(bytes.as_ptr() as _, batch_size) };

        items.to_vec()
    }
}

impl<T: NativePType> PrimitiveAccessor<T> {
    pub fn new(array: PrimitiveArray) -> Self {
        Self {
            array,
            _marker: PhantomData,
        }
    }
}

impl<T: Copy + std::fmt::Debug> Iterator for ArrayIter<T> {
    type Item = Option<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.cached_batch.as_ref() {
                None => {
                    self.batch_idx = 0;
                    self.cached_batch = Some(self.accessor.decode_batch(self.overall_idx, None))
                }
                Some(batch) => {
                    if self.overall_idx == self.accessor.len() {
                        return None;
                    } else if !self.accessor.is_valid(self.overall_idx) {
                        self.overall_idx += 1;
                        self.batch_idx += 1;
                        return Some(None);
                    } else if !(self.batch_idx == batch.len()) {
                        let i = batch[self.batch_idx];
                        self.overall_idx += 1;
                        self.batch_idx += 1;
                        return Some(Some(i));
                    } else if self.batch_idx == batch.len() {
                        self.cached_batch.take();
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.accessor.len() - self.overall_idx,
            Some(self.accessor.len() - self.overall_idx),
        )
    }
}

impl<T: Copy + std::fmt::Debug> ExactSizeIterator for ArrayIter<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::variants::ArrayVariants;

    #[test]
    fn iter_example() {
        let array = PrimitiveArray::from_nullable_vec(vec![Some(1.0_f32); 1025]);
        let array_iter = array.as_primitive_array_unchecked().float32_iter().unwrap();

        let mut counter = 0;

        for _f in array_iter {
            counter += 1;
        }

        assert_eq!(counter, 1025);
    }
}
