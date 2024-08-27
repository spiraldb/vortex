use std::borrow::Cow;
use std::ops::Deref;

pub use adapter::*;
pub use ext::*;
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::validity::Validity;
use crate::Array;

mod adapter;
mod ext;

pub const BATCH_SIZE: usize = 1024;

/// A stream of array chunks along with a DType.
/// Analogous to Arrow's RecordBatchReader.
pub trait ArrayIterator: Iterator<Item = VortexResult<Array>> {
    fn dtype(&self) -> &DType;
}

pub trait Accessor<T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    fn array_len(&self) -> usize;
    fn is_valid(&self, index: usize) -> bool;
    fn is_null(&self, index: usize) -> bool {
        !self.is_valid(index)
    }
    fn value_unchecked(&self, index: usize) -> T;
    fn array_validity(&self) -> Validity {
        todo!("should probably be empty")
    }

    #[allow(clippy::uninit_vec)]
    #[inline]
    fn decode_batch(&self, start_idx: usize) -> Cow<'_, [T]> {
        let batch_size = BATCH_SIZE.min(self.array_len() - start_idx);

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

/// Iterate over batches of compressed arrays, should help with writing vectorized code.
/// Note that it doesn't respect per-item validity, and the per-item `Validity` instance should be advised
/// for correctness, must "high-performance" code will ignore the validity when doing work, and will only
/// re-use it when reconstructing the result array.
pub struct VectorizedArrayIter<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    accessor: &'a dyn Accessor<T>,
    validity: Validity,
    current_idx: usize,
    len: usize,
}

impl<'a, T> VectorizedArrayIter<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    T: Copy,
{
    pub fn new(accessor: &'a dyn Accessor<T>) -> Self {
        let len = accessor.array_len();
        let validity = accessor.array_validity();

        Self {
            accessor,
            len,
            validity,
            current_idx: 0,
        }
    }
}

pub struct Batch<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    T: Sized,
{
    data: BatchData<'a, T>,
    validity: Validity,
}

impl<'a, T> Batch<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    T: Sized,
{
    pub fn new(data: &'a [T], validity: Validity) -> Self {
        let data = if data.len() == BATCH_SIZE {
            BatchData::Fixed(data.try_into().unwrap())
        } else {
            BatchData::Variable(Cow::Borrowed(data))
        };

        Self { data, validity }
    }

    pub fn new_from_cow(data: Cow<'a, [T]>, validity: Validity) -> Self {
        let data = match data {
            Cow::Borrowed(b) if b.len() == BATCH_SIZE => BatchData::Fixed(b.try_into().unwrap()),
            _ => BatchData::Variable(data),
        };
        Self { data, validity }
    }

    #[inline]
    pub fn is_valid(&self, index: usize) -> bool {
        self.validity.is_valid(index)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Safety
    /// ok
    #[inline]
    pub unsafe fn get_unchecked(&self, index: usize) -> &T {
        unsafe { self.data.get_unchecked(index) }
    }
}

pub struct FlattenedBatch<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    inner: Batch<'a, T>,
    current: usize,
}

impl<'a, T> Iterator for FlattenedBatch<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
    T: Copy,
{
    type Item = Option<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.inner.len() {
            None
        } else if !self.inner.is_valid(self.current) {
            self.current += 1;
            Some(None)
        } else {
            let old = self.current;
            self.current += 1;
            Some(Some(unsafe { *self.inner.get_unchecked(old) }))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.inner.len() - self.current,
            Some(self.inner.len() - self.current),
        )
    }
}

impl<'a, T> IntoIterator for Batch<'a, T>
where
    T: Copy,
{
    type Item = Option<T>;
    type IntoIter = FlattenedBatch<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        FlattenedBatch {
            inner: self,
            current: 0,
        }
    }
}

pub enum BatchData<'a, T>
where
    T: Sized,
    [T]: ToOwned<Owned = Vec<T>>,
{
    Fixed(&'a [T; BATCH_SIZE]),
    Variable(Cow<'a, [T]>),
}

impl<'a, T> Deref for BatchData<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        match self {
            BatchData::Fixed(f) => &**f,
            BatchData::Variable(v) => v.as_ref(),
        }
    }
}

impl<'a, T: Copy> Iterator for VectorizedArrayIter<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    type Item = Batch<'a, T>;

    #[allow(clippy::unwrap_in_result)]
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_idx == self.accessor.array_len() {
            None
        } else {
            let data = self.accessor.decode_batch(self.current_idx);

            let validity = self
                .validity
                .slice(self.current_idx, self.current_idx + data.len())
                .expect("The slice bounds should always be within the array's limits");
            self.current_idx += data.len();

            let batch = Batch::new_from_cow(data, validity);

            Some(batch)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.len - self.current_idx,
            Some(self.len - self.current_idx),
        )
    }
}

impl<T: Copy> ExactSizeIterator for VectorizedArrayIter<'_, T> {}

#[cfg(test)]
mod tests {
    use crate::array::PrimitiveArray;
    use crate::variants::ArrayVariants;

    #[test]
    fn iter_example() {
        let array =
            PrimitiveArray::from_nullable_vec((0..1_000_000).map(|v| Some(v as f32)).collect());
        let array_iter = array
            .as_primitive_array_unchecked()
            .float32_iter()
            .unwrap()
            .flatten();

        for (idx, v) in array_iter.enumerate() {
            assert_eq!(idx as f32, v.unwrap());
        }
    }
}
