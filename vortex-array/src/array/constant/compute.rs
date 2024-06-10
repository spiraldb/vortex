use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::{Array, IntoArray};
use crate::array::constant::ConstantArray;
use crate::compute::ArrayCompute;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::take::TakeFn;

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
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        Ok(Self::new(self.scalar().clone(), indices.len()).into_array())
    }
}
