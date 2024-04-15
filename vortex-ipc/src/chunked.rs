use lending_iterator::prelude::*;
use vortex::{Array, OwnedArray};
use vortex_error::VortexResult;
use vortex_schema::DType;

/// Stream chunks of a Vortex array.
#[allow(dead_code)]
pub trait ArrayChunkReader: Iterator<Item = VortexResult<OwnedArray>> {
    fn dtype(&self) -> &DType;
}

#[allow(dead_code)]
pub trait ArrayViewChunkReader: LendingIteratorDyn<Item = HKT!(VortexResult<Array<'_>>)> {
    fn dtype(&self) -> &DType;
}
