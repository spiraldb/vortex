use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayRef};
use crate::compute::take::TakeFn;
use crate::error::EncResult;

impl TakeFn for ConstantArray {
    fn take(&self, indices: &dyn Array) -> EncResult<ArrayRef> {
        Ok(ConstantArray::new(dyn_clone::clone_box(self.scalar()), indices.len()).boxed())
    }
}
