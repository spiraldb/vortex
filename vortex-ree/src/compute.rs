use crate::REEArray;
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::error::VortexResult;
use vortex::scalar::Scalar;

impl ArrayCompute for REEArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for REEArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        scalar_at(self.values(), self.find_physical_index(index)?)
    }
}
