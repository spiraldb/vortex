use crate::array::bool::BoolArray;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::compare::CompareFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::slice::SliceFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;

mod as_arrow;
mod compare;
mod fill;
mod flatten;
mod scalar_at;
mod slice;
mod take;

impl ArrayCompute for BoolArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

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
