use crate::array::bool::BoolArray;
use crate::array::{Array, ArrayRef};
use crate::compute::take::TakeFn;
use vortex_error::VortexResult;

impl TakeFn for BoolArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        todo!()
    }
}
