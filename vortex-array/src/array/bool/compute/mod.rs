use crate::array::bool::BoolArray;
use crate::compute::unary::{FillForwardFn, ScalarAtFn};
use crate::compute::{ArrayCompute, CompareFn, SliceFn, TakeFn};

mod compare;
mod fill;
mod filter;
mod flatten;
mod scalar_at;
mod slice;
mod take;

impl ArrayCompute for BoolArray {
    fn compare(&self) -> Option<&dyn CompareFn> {
        Some(self)
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
}
