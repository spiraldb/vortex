use vortex_error::VortexResult;

use crate::array2::compute::ArrayCompute;
use crate::array2::ree::REEArray;
use crate::array2::ScalarAtFn;
use crate::scalar::Scalar;

impl ArrayCompute for &dyn REEArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for &dyn REEArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        todo!()
    }
}
