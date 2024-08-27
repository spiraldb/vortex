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
        let batch_size = DEFAULT_BATCH_SIZE.min(self.array_len() - start_idx);

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

impl<'a, T: Copy> Iterator for VectorizedArrayIter<'a, T>
where
    [T]: ToOwned<Owned = Vec<T>>,
{
    type Item = (Cow<'a, [T]>, Validity);

    #[allow(clippy::unwrap_in_result)]
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_idx == self.accessor.array_len() {
            None
        } else {
            let batch = self.accessor.decode_batch(self.current_idx);

            let validity = self
                .validity
                .slice(self.current_idx, self.current_idx + batch.len())
                .expect("The slice bounds should always be within the array's limits");
            self.current_idx += batch.len();

            Some((batch, validity))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.len - self.current_idx,
            Some(self.len - self.current_idx),
        )
    }
}

// impl IntoIterator for (Cow<'_, [T])

impl<T: Copy> ExactSizeIterator for VectorizedArrayIter<'_, T> {}

#[cfg(test)]
mod tests {
    // use crate::array::PrimitiveArray;
    // use crate::variants::ArrayVariants;

    // #[test]
    // fn iter_example() {
    //     let array =
    //         PrimitiveArray::from_nullable_vec((0..1_000_000).map(|v| Some(v as f32)).collect());
    //     let array_iter = array
    //         .as_primitive_array_unchecked()
    //         .float32_iter()
    //         .unwrap()

    //     for (idx, v) in array_iter.enumerate() {
    //         assert_eq!(idx as f32, v.unwrap());
    //     }
    // }
}
