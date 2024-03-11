use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayRef};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::Scalar;

impl ArrayCompute for ConstantArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for ConstantArray {
    fn scalar_at(&self, _index: usize) -> VortexResult<Scalar> {
        Ok(self.scalar().clone())
    }
}

impl TakeFn for ConstantArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        Ok(ConstantArray::new(self.scalar().clone(), indices.len()).boxed())
    }
}
