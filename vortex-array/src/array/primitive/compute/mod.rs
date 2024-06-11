use crate::array::primitive::PrimitiveArray;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::cast::CastFn;
use crate::compute::compare::CompareFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::filter_indices::FilterIndicesFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::scalar_subtract::SubtractScalarFn;
use crate::compute::search_sorted::SearchSortedFn;
use crate::compute::slice::SliceFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;

mod as_arrow;
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
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

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
