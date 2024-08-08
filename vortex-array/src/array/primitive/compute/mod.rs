use crate::array::primitive::PrimitiveArray;
use crate::compute::unary::{CastFn, FillForwardFn, ScalarAtFn, SubtractScalarFn};
use crate::compute::{ArrayCompute, CompareFn, FilterIndicesFn, SearchSortedFn, SliceFn, TakeFn};

mod cast;
mod compare;
mod fill;
mod filter;
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
