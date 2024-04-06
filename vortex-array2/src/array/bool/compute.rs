use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::compute::{ArrayCompute, ScalarAtFn};

impl ArrayCompute for BoolArray<'_> {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for BoolArray<'_> {
    fn scalar_at(&self, _index: usize) -> VortexResult<Scalar> {
        todo!()
    }
}
