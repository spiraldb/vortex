use vortex::array::ArrayRef;
use vortex_error::VortexResult;
use vortex_schema::DType;

/// Stream chunks of a Vortex array.
pub trait ArrayChunkReader: Iterator<Item = VortexResult<ArrayRef>> {
    fn dtype(&self) -> &DType;
}
