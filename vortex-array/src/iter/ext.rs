use itertools::Itertools;
use vortex_error::VortexResult;

use crate::array::ChunkedArray;
use crate::iter::ArrayIterator;
use crate::stream::{ArrayStream, ArrayStreamAdapter};

pub trait ArrayIteratorExt: ArrayIterator {
    fn into_stream(self) -> impl ArrayStream
    where
        Self: Sized,
    {
        ArrayStreamAdapter::new(self.dtype().clone(), futures_util::stream::iter(self))
    }

    fn try_into_chunked(self) -> VortexResult<ChunkedArray>
    where
        Self: Sized,
    {
        let dtype = self.dtype().clone();
        ChunkedArray::try_new(self.try_collect()?, dtype)
    }
}
