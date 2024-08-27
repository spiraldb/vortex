use std::borrow::Cow;
use std::marker::PhantomData;

pub use adapter::*;
pub use ext::*;
use vortex_dtype::DType;
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
pub struct ArrayIter<'a, A, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    A: Accessor<'a, T>,
{
    accessor: A,
    cached_batch: Cow<'a, [T]>,
    batch_idx: usize,
    overall_idx: usize,
    len: usize,
}

pub struct PrimitiveAccessor<'a, T> {
    array: &'a PrimitiveArray,
    validity: Validity,
    _marker: PhantomData<T>,
}

impl<'a, T> PrimitiveAccessor<'a, T> {
    pub fn new(array: &'a PrimitiveArray) -> Self {
        let validity = array.validity();

        Self {
            array,
            validity,
            _marker: PhantomData,
        }
    }
}

impl<'a, T> Accessor<'a, T> for PrimitiveAccessor<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    T: Copy,
{
    fn len(&self) -> usize {
        self.array.len()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity.is_valid(index)
    }

    fn value_unchecked(&self, index: usize) -> T {
        self.array.buffer().typed::<T>()[index]
    }

    fn decode_batch(&self, start_idx: usize) -> Cow<'a, [T]> {
        let batch_size = usize::min(1024, self.array.len() - start_idx);

        Cow::Borrowed(&self.array.buffer().typed()[start_idx..start_idx + batch_size])
    }
}

impl<'a, A, T> ArrayIter<'a, A, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    A: Accessor<'a, T>,
    T: Copy,
{
    pub fn new(accessor: A) -> Self {
        let len = accessor.len();

        let cached_batch = accessor.decode_batch(0);

        Self {
            accessor,
            len,
            cached_batch,
            batch_idx: 0,
            overall_idx: 0,
        }
    }

    #[inline]
    pub fn maybe_load_batch(&mut self) {
        if self.batch_idx == self.cached_batch.len() {
            self.overall_idx += self.batch_idx;
            self.batch_idx = 0;
            self.cached_batch = self.accessor.decode_batch(self.overall_idx);
        }
    }
}

#[allow(clippy::len_without_is_empty)]
pub trait Accessor<'u, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    fn len(&self) -> usize;
    fn is_valid(&self, index: usize) -> bool;
    fn is_null(&self, index: usize) -> bool {
        !self.is_valid(index)
    }
    fn value_unchecked(&self, index: usize) -> T;

    #[allow(clippy::uninit_vec)]
    #[inline]
    fn decode_batch(&self, start_idx: usize) -> Cow<'u, [T]> {
        let batch_size = DEFAULT_BATCH_SIZE.min(self.len() - start_idx);

        let mut batch = Vec::with_capacity(batch_size);

        // Safety:
        // We've made sure that we have at least `batch_size` elements to put into
        // the vector and sufficient capacity.
        unsafe {
            batch.set_len(batch_size);
        }

        for (idx, batch_item) in batch.iter_mut().enumerate().take(batch_size) {
            *batch_item = self.value_unchecked(start_idx + idx);
        }
        Cow::Owned(batch)
    }
}

impl<'a, A, T: Copy> Iterator for ArrayIter<'a, A, T>
where
    A: Accessor<'a, T>,
{
    type Item = Option<T>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.len == self.overall_idx + self.batch_idx {
            return None;
        }

        let v = if !self.accessor.is_valid(self.overall_idx + self.batch_idx) {
            Some(None)
        } else {
            self.maybe_load_batch();
            let i = unsafe { *self.cached_batch.get_unchecked(self.batch_idx) };

            Some(Some(i))
        };

        self.batch_idx += 1;
        v
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let current = self.batch_idx + self.overall_idx;
        (self.len - current, Some(self.len - current))
    }
}

impl<'a, A: Accessor<'a, T>, T: Copy> ExactSizeIterator for ArrayIter<'a, A, T> {}

#[cfg(test)]
mod tests {
    use crate::array::PrimitiveArray;
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
