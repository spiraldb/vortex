use std::borrow::Cow;

pub use adapter::*;
pub use ext::*;
use vortex_dtype::DType;
use vortex_error::VortexResult;

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
pub struct ArrayIter<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    accessor: &'a dyn Accessor<T>,
    cached_batch: Cow<'a, [T]>,
    batch_idx: usize,
    overall_idx: usize,
    len: usize,
    validity: Validity,
}

impl<'a, T> ArrayIter<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    pub fn new(accessor: &'a impl Accessor<T>) -> Self {
        let len = accessor.len();
        let validity = accessor.validity();

        let cached_batch = accessor.decode_batch(0);

        Self {
            accessor,
            len,
            validity,
            cached_batch,
            batch_idx: 0,
            overall_idx: 0,
        }
    }

    #[inline]
    pub fn load_batch(&mut self) {
        self.overall_idx += self.batch_idx;
        self.batch_idx = 0;
        self.cached_batch = self.accessor.decode_batch(self.overall_idx);
    }
}

pub trait Accessor<T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn is_valid(&self, index: usize) -> bool;
    fn value_unchecked(&self, index: usize) -> T;
    fn validity(&self) -> Validity {
        todo!()
    }

    #[inline]
    fn decode_batch(&self, _start_idx: usize) -> Cow<'_, [T]> {
        // let batch_size = DEFAULT_BATCH_SIZE.min(self.len() - start_idx);
        // if batch.capacity() < batch_size {
        //     batch.reserve(batch_size - batch.capacity());
        // }

        // // Safety:
        // // We make sure above that we have at least `batch_size` elements to put into
        // // the vector and sufficient capacity.
        // unsafe {
        //     batch.set_len(batch_size);
        // }

        // for (idx, batch_item) in batch.iter_mut().enumerate().take(batch_size) {
        //     *batch_item = self.value_unchecked(start_idx + idx);
        // }
        todo!()
    }
}

impl<'a, T: Copy> Iterator for ArrayIter<'a, T> {
    type Item = Option<T>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.overall_idx + self.batch_idx == self.len {
            return None;
        }

        if !self.validity.is_valid(self.overall_idx + self.batch_idx) {
            self.batch_idx += 1;
            return Some(None);
        } else if self.batch_idx == self.cached_batch.len() {
            self.load_batch();
        }

        let i = unsafe { *self.cached_batch.get_unchecked(self.batch_idx) };
        self.batch_idx += 1;

        Some(Some(i))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.len - self.overall_idx,
            Some(self.len - self.overall_idx),
        )
    }
}

// impl<T: Copy> ExactSizeIterator for ArrayIter<'_, T> {}

#[cfg(test)]
mod tests {
    use super::*;
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
