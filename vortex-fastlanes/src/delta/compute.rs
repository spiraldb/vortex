use vortex::compute::ArrayCompute;
use vortex::compute::flatten::{FlattenedArray, FlattenFn};
use vortex::error::VortexResult;

use crate::delta::compress::decompress;
use crate::DeltaArray;

impl ArrayCompute for DeltaArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }
}

impl FlattenFn for DeltaArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        decompress(self).map(FlattenedArray::Primitive)
    }
}
