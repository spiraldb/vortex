use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::compute::{ArrayCompute, ScalarAtFn};
use crate::ree::REEArray;

impl ArrayCompute for &dyn REEArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for &dyn REEArray {
    fn scalar_at(&self, _index: usize) -> VortexResult<Scalar> {
        todo!()
    }
}
