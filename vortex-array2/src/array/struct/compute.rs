use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::array::r#struct::StructArray;
use crate::compute::{ArrayCompute, ScalarAtFn};

impl ArrayCompute for StructArray<'_> {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for StructArray<'_> {
    fn scalar_at(&self, _index: usize) -> VortexResult<Scalar> {
        todo!()
    }
}
