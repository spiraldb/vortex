use cast::CastPrimitiveFn;
use patch::PatchFn;
use scalar_at::ScalarAtFn;
use take::TakeFn;

pub mod add;
pub mod as_contiguous;
pub mod cast;
pub mod patch;
pub mod repeat;
pub mod scalar_at;
pub mod search_sorted;
pub mod take;

pub trait ArrayCompute {
    fn cast_primitive(&self) -> Option<&dyn CastPrimitiveFn> {
        None
    }

    fn patch(&self) -> Option<&dyn PatchFn> {
        None
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        None
    }
    fn take(&self) -> Option<&dyn TakeFn> {
        None
    }
}
