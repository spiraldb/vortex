use crate::array::constant::ConstantArray;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;

impl ArrayCompute for ConstantArray {
    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}
