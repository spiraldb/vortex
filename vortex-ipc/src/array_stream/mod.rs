use std::future::ready;

pub use adapter::*;
pub use ext::*;
use futures_util::Stream;
use futures_util::{stream, StreamExt};
pub use take_rows::*;
use vortex::array::chunked::ChunkedArray;
use vortex::{Array, ArrayDType};
use vortex_dtype::DType;
use vortex_error::VortexResult;

mod adapter;
mod ext;
mod take_rows;

/// A stream of array chunks along with a DType.
///
/// Can be thought of as equivalent to Arrow's RecordBatchReader.
pub trait ArrayStream: Stream<Item = VortexResult<Array>> {
    fn dtype(&self) -> &DType;
}

// TODO(ngates): implement these fns on Array / ChunkedArray when we move ArrayStream into main crate
pub struct ArrayStreamFactory;
impl ArrayStreamFactory {
    pub fn from_array(array: Array) -> impl ArrayStream {
        ArrayStreamAdapter::new(array.dtype().clone(), stream::once(ready(Ok(array))))
    }

    pub fn from_chunked_array(array: &ChunkedArray) -> impl ArrayStream + '_ {
        ArrayStreamAdapter::new(array.dtype().clone(), stream::iter(array.chunks()).map(Ok))
    }
}
