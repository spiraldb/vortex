use crate::array::BoolArray;
use crate::compute::unary::{CastFn, FillForwardFn, ScalarAtFn};
use crate::compute::{AndFn, ArrayCompute, OrFn, SliceFn, TakeFn};

mod boolean;

mod cast;
mod fill;
mod filter;
mod flatten;
mod scalar_at;
mod slice;
mod take;

impl ArrayCompute for BoolArray {
    fn cast(&self) -> Option<&dyn CastFn> {
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

    fn and(&self) -> Option<&dyn AndFn> {
        Some(self)
    }

    fn or(&self) -> Option<&dyn OrFn> {
        Some(self)
    }
}
