use crate::DictArray;
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::error::VortexResult;
use vortex::scalar::Scalar;

impl ArrayCompute for DictArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for DictArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        let dict_index: usize = scalar_at(self.codes(), index)?.try_into()?;
        scalar_at(self.dict(), dict_index)
    }
}
