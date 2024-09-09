use vortex_error::VortexResult;

use crate::array::BoolArray;
use crate::compute::unary::{FillForwardFn, ScalarAtFn};
use crate::compute::{AndFn, ArrayCompute, CompareFn, Operator, OrFn, SliceFn, TakeFn};
use crate::Array;

mod boolean;
mod compare;
mod fill;
mod filter;
mod flatten;
mod scalar_at;
mod slice;
mod take;

impl ArrayCompute for BoolArray {
    fn compare(&self, array: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        Some(CompareFn::compare(self, array, operator))
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }

    fn and(&self) -> Option<&dyn AndFn> {
        Some(self)
    }

    fn or(&self) -> Option<&dyn OrFn> {
        Some(self)
    }
}
