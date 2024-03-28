use lending_iterator::prelude::*;
use vortex::array::{Array, ArrayRef};
use vortex_error::VortexResult;
use vortex_schema::DType;

/// Stream chunks of a Vortex array.
#[allow(dead_code)]
pub trait ArrayChunkReader: Iterator<Item = VortexResult<ArrayRef>> {
    fn dtype(&self) -> &DType;
}

pub trait ArrayViewChunkReader: LendingIteratorDyn<Item = HKT!(VortexResult<&dyn Array>)> {
    fn dtype(&self) -> &DType;
}
