use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::array::ree::REEArray;
use crate::compute::{ArrayCompute, ScalarAtFn};

impl ArrayCompute for REEArray<'_> {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for REEArray<'_> {
    fn scalar_at(&self, _index: usize) -> VortexResult<Scalar> {
        todo!()
    }
}
