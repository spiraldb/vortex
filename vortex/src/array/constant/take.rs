use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayRef};
use crate::compute::take::TakeFn;
use crate::error::VortexResult;

impl TakeFn for ConstantArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        Ok(ConstantArray::new(dyn_clone::clone_box(self.scalar()), indices.len()).boxed())
    }
}
