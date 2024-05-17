mod adapter;
mod ext;
pub use adapter::*;
pub use ext::*;
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::Array;

/// A stream of array chunks along with a DType.
/// Analogous to Arrow's RecordBatchReader.
pub trait ArrayIterator: Iterator<Item = VortexResult<Array>> {
    fn dtype(&self) -> &DType;
}
