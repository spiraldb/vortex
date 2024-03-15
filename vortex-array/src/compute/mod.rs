use as_contiguous::AsContiguousFn;
use cast::CastFn;
use fill::FillForwardFn;
use flatten::{FlattenBoolFn, FlattenPrimitiveFn};
use patch::PatchFn;
use scalar_at::ScalarAtFn;
use search_sorted::SearchSortedFn;
use take::TakeFn;

pub mod add;
pub mod as_contiguous;
pub mod cast;
pub mod fill;
pub mod flatten;
pub mod patch;
pub mod repeat;
pub mod scalar_at;
pub mod search_sorted;
pub mod take;

pub trait ArrayCompute {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        None
    }

    fn cast(&self) -> Option<&dyn CastFn> {
        None
    }

    fn flatten_bool(&self) -> Option<&dyn FlattenBoolFn> {
        None
    }

    fn flatten_primitive(&self) -> Option<&dyn FlattenPrimitiveFn> {
        None
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        None
    }

    fn patch(&self) -> Option<&dyn PatchFn> {
        None
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        None
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        None
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        None
    }
}
