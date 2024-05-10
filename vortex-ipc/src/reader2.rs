use futures_util::Stream;
use vortex::OwnedArray;
use vortex_dtype::DType;
use vortex_error::VortexResult;

/// A stream of array chunks along with a DType.
pub trait ArrayReader: Stream<Item = VortexResult<OwnedArray>> {
    #[allow(dead_code)]
    fn dtype(&self) -> &DType;
}
