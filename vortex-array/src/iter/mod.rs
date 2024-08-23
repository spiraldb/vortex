use std::marker::PhantomData;

pub use adapter::*;
pub use ext::*;
use vortex_buffer::Buffer;
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

#[allow(dead_code)]
pub struct ArrayIter<'a, T> {
    accessor: &'a dyn Accessor<T>,
    cached_batch: Option<Vec<T>>,
    batch_idx: usize,
    overall_idx: usize,
    len: usize,
    validity: Validity,
}

impl<'a, T> ArrayIter<'a, T> {
    pub fn new(accessor: &'a impl Accessor<T>) -> Self {
        let len = accessor.len();
        let validity = accessor.validity();

        Self {
            accessor,
            len,
            validity,
            cached_batch: None,
            batch_idx: 0,
            overall_idx: 0,
        }
    }

    #[inline(never)]
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

pub trait Accessor<T> {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn is_valid(&self, index: usize) -> bool;
    fn value_unchecked(&self, index: usize) -> T;
    fn validity(&self) -> Validity {
        todo!()
    }

    #[inline(never)]
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

        for (idx, batch_item) in batch.iter_mut().enumerate().take(batch_size) {
            *batch_item = self.value_unchecked(start_idx + idx);
        }
    }
}

pub struct PrimitiveAccessor<'a, T> {
    buffer: &'a Buffer,
    validity: Validity,
    len: usize,
    _marker: PhantomData<T>,
}

impl<'a, N: NativePType> Accessor<N> for PrimitiveAccessor<'a, N> {
    #[inline(never)]
    fn len(&self) -> usize {
        self.len
    }

    #[inline(never)]
    fn is_valid(&self, index: usize) -> bool {
        self.validity.is_valid(index)
    }

    #[inline(never)]
    fn validity(&self) -> Validity {
        self.validity.clone()
    }

    #[inline(never)]
    fn value_unchecked(&self, index: usize) -> N {
        let start = index * std::mem::size_of::<N>();
        let end = (index + 1) * std::mem::size_of::<N>();
        N::try_from_le_bytes(&self.buffer[start..end]).unwrap()
    }

    #[inline(never)]
    fn decode_batch(&self, start_idx: usize, batch: &mut Vec<N>) {
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

        let start = start_idx * std::mem::size_of::<N>();

        unsafe {
            let bytes_ptr = self.buffer.as_ptr().add(start) as _;
            std::ptr::copy_nonoverlapping(bytes_ptr, batch.as_mut_ptr(), batch_size);
        }
    }
}

impl<'a, T: NativePType> PrimitiveAccessor<'a, T> {
    pub fn new(array: &'a PrimitiveArray) -> Self {
        let validity = array.validity();
        let len = array.len();
        let buffer = array.buffer();

        Self {
            buffer,
            validity,
            len,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: Copy> Iterator for ArrayIter<'a, T> {
    type Item = Option<T>;

    #[allow(clippy::unwrap_in_result)]
    #[inline(never)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.cached_batch.is_none() {
            self.load_batch();
        }

        if self.overall_idx == self.len {
            return None;
        }

        let batch_len = self.cached_batch.as_ref().map(Vec::len).unwrap();

        if !self.validity.is_valid(self.overall_idx) {
            self.overall_idx += 1;
            self.batch_idx += 1;
            return Some(None);
        }

        if self.batch_idx == batch_len {
            self.load_batch();
        };

        let i = unsafe {
            self.cached_batch
                .as_ref()
                .unwrap()
                .get_unchecked(self.batch_idx)
        };

        self.overall_idx += 1;
        self.batch_idx += 1;

        Some(Some(*i))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.len - self.overall_idx,
            Some(self.len - self.overall_idx),
        )
    }
}

impl<T: Copy> ExactSizeIterator for ArrayIter<'_, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::variants::ArrayVariants;

    #[test]
    fn iter_example() {
        let array =
            PrimitiveArray::from_nullable_vec((0..1_000_000).map(|v| Some(v as f32)).collect());
        let array_iter = array.as_primitive_array_unchecked().float32_iter().unwrap();

        for (idx, v) in array_iter.enumerate() {
            assert_eq!(idx as f32, v.unwrap());
        }
    }
}
