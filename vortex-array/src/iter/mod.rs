use std::marker::PhantomData;
use std::sync::Arc;

pub use adapter::*;
pub use ext::*;
use vortex_dtype::{DType, NativePType};
use vortex_error::VortexResult;

use crate::array::PrimitiveArray;
use crate::validity::Validity;
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

    pub fn load_batch(&mut self) {
        self.batch_idx = 0;
        let mut v = self
            .cached_batch
            .take()
            .unwrap_or_else(|| Vec::with_capacity(DEFAULT_BATCH_SIZE));
        self.accessor.decode_batch(self.overall_idx, &mut v);
        self.cached_batch = Some(v)
    }
}

#[allow(dead_code)]
pub struct BatchedIter<T> {
    accessor: Arc<dyn Accessor<T>>,
    overall_idx: usize,
}

pub enum Batch<T> {
    Full([T; 1024]),
    Partial(Vec<T>),
}

impl<T: Copy> From<&[T]> for Batch<T> {
    fn from(value: &[T]) -> Self {
        if value.len() == 1024 {
            Self::Full(<[T; 1024]>::try_from(value).unwrap())
        } else {
            Self::Partial(value.to_vec())
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
    fn decode_batch(&self, start_idx: usize, batch: &mut Vec<O>) {
        let batch_size = DEFAULT_BATCH_SIZE.min(self.len() - start_idx);
        if batch.capacity() < batch_size {
            batch.reserve(batch_size - batch.capacity());
        }

        // Safety:
        // We make sure above that we have at least `batch_size` elements to put into
        // the vector and sufficient capacity.
        unsafe {
            batch.set_len(batch_size);
        }

        for (idx, batch_item) in batch.iter_mut().enumerate().take(batch_size) {
            *batch_item = self.value_unchecked(start_idx + idx);
        }
    }
}

pub struct PrimitiveAccessor<T> {
    array: PrimitiveArray,
    validity: Validity,
    _marker: PhantomData<T>,
}

impl<T: NativePType> Accessor<T> for PrimitiveAccessor<T> {
    fn len(&self) -> usize {
        self.array.len()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity.is_valid(index)
    }

    fn value_unchecked(&self, index: usize) -> T {
        let start = index * std::mem::size_of::<T>();
        let end = (index + 1) * std::mem::size_of::<T>();
        T::try_from_le_bytes(&self.array.buffer()[start..end]).unwrap()
    }

    fn decode_batch(&self, start_idx: usize, batch: &mut Vec<T>) {
        let batch_size = DEFAULT_BATCH_SIZE.min(self.len() - start_idx);
        if batch.capacity() < batch_size {
            batch.reserve(batch_size - batch.capacity());
        }

        // Safety:
        // We make sure above that we have at least `batch_size` elements to put into
        // the vector and sufficient capacity.
        unsafe {
            batch.set_len(batch_size);
        }

        let start = start_idx * std::mem::size_of::<T>();
        let end = (start_idx + batch_size) * std::mem::size_of::<T>();

        let bytes = &self.array.buffer()[start..end];

        let bytes_ptr = bytes.as_ptr() as *const T;

        unsafe {
            std::ptr::copy_nonoverlapping(bytes_ptr, batch.as_mut_ptr(), batch_size);
        }
    }
}

impl<T: NativePType> PrimitiveAccessor<T> {
    pub fn new(array: PrimitiveArray) -> Self {
        let validity = array.validity();

        Self {
            array,
            validity,
            _marker: PhantomData,
        }
    }
}

impl<T: Copy> Iterator for ArrayIter<T> {
    type Item = Option<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.cached_batch.as_ref() {
                None => {
                    self.load_batch();
                }
                Some(batch) => {
                    if self.overall_idx == self.accessor.len() {
                        return None;
                    } else if !self.accessor.is_valid(self.overall_idx) {
                        self.overall_idx += 1;
                        self.batch_idx += 1;
                        return Some(None);
                    } else if self.batch_idx != batch.len() {
                        let i = batch[self.batch_idx];
                        self.overall_idx += 1;
                        self.batch_idx += 1;
                        return Some(Some(i));
                    } else if self.batch_idx == batch.len() {
                        self.load_batch()
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

impl<T: Copy> ExactSizeIterator for ArrayIter<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::variants::ArrayVariants;

    #[test]
    fn iter_example() {
        let array = PrimitiveArray::from_nullable_vec((0..1025).map(|v| Some(v as f32)).collect());
        let array_iter = array.as_primitive_array_unchecked().float32_iter().unwrap();

        for (idx, v) in array_iter.enumerate() {
            assert_eq!(idx as f32, v.unwrap());
        }
    }
}
