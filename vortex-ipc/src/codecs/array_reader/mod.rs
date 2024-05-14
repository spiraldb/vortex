pub use adapter::*;
pub use ext::*;
use futures_util::Stream;
use vortex::Array;
use vortex_dtype::DType;
use vortex_error::VortexResult;

mod adapter;
mod ext;
mod take_rows;

/// A stream of array chunks along with a DType.
///
/// Can be thought of as equivalent to Arrow's RecordBatchReader.
pub trait ArrayReader: Stream<Item = VortexResult<Array>> {
    fn dtype(&self) -> &DType;
}
