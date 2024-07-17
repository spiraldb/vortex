use crate::array::primitive::PrimitiveArray;
use crate::compute::unary::cast::CastFn;
use crate::compute::unary::fill_forward::FillForwardFn;
use crate::compute::unary::scalar_at::ScalarAtFn;
use crate::compute::unary::scalar_subtract::SubtractScalarFn;
use crate::compute::FilterIndicesFn;
use crate::compute::SearchSortedFn;
use crate::compute::{ArrayCompute, CompareFn, SliceFn, TakeFn};

mod cast;
mod compare;
mod fill;
mod filter_indices;
mod scalar_at;
mod search_sorted;
mod slice;
mod subtract_scalar;
mod take;

impl ArrayCompute for PrimitiveArray {
    fn cast(&self) -> Option<&dyn CastFn> {
        Some(self)
    }

    fn compare(&self) -> Option<&dyn CompareFn> {
        Some(self)
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        Some(self)
    }
    fn filter_indices(&self) -> Option<&dyn FilterIndicesFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn subtract_scalar(&self) -> Option<&dyn SubtractScalarFn> {
        Some(self)
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}
