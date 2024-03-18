use crate::array::primitive::PrimitiveArray;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::cast::CastFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::flatten::FlattenFn;
use crate::compute::patch::PatchFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::search_sorted::SearchSortedFn;
use crate::compute::ArrayCompute;

mod as_arrow;
mod as_contiguous;
mod cast;
mod fill;
mod flatten;
mod patch;
mod scalar_at;
mod search_sorted;

impl ArrayCompute for PrimitiveArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn cast(&self) -> Option<&dyn CastFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        Some(self)
    }

    fn patch(&self) -> Option<&dyn PatchFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        Some(self)
    }
}
