use itertools::Itertools;
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::Array;

/// A stream of array chunks along with a DType.
/// Analogous to Arrow's RecordBatchReader.
pub trait ArrayIterator: Iterator<Item = VortexResult<Array>> {
    fn dtype(&self) -> &DType;
}

pub struct ArrayIteratorAdapter<I> {
    dtype: DType,
    inner: I,
}

impl<I> ArrayIteratorAdapter<I> {
    pub fn new(dtype: DType, inner: I) -> Self {
        Self { dtype, inner }
    }
}

impl<I> Iterator for ArrayIteratorAdapter<I>
where
    I: Iterator<Item = VortexResult<Array>>,
{
    type Item = VortexResult<Array>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<I> ArrayIterator for ArrayIteratorAdapter<I>
where
    I: Iterator<Item = VortexResult<Array>>,
{
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

pub trait ArrayIteratorExt: ArrayIterator {
    fn try_into_chunked(self) -> VortexResult<ChunkedArray>
    where
        Self: Sized,
    {
        let dtype = self.dtype().clone();
        ChunkedArray::try_new(self.try_collect()?, dtype)
    }
}
