use compare::CompareFn;
use search_sorted::SearchSortedFn;
use slice::SliceFn;
use take::TakeFn;

use self::filter_indices::FilterIndicesFn;
use self::unary::cast::CastFn;
use self::unary::fill_forward::FillForwardFn;
use self::unary::scalar_at::ScalarAtFn;
use self::unary::scalar_subtract::SubtractScalarFn;

mod arith;
pub mod compare;
pub mod filter_indices;
pub mod search_sorted;
pub mod slice;
pub mod take;
pub mod unary;

pub trait ArrayCompute {
    fn cast(&self) -> Option<&dyn CastFn> {
        None
    }

    fn compare(&self) -> Option<&dyn CompareFn> {
        None
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        None
    }

    fn filter_indices(&self) -> Option<&dyn FilterIndicesFn> {
        None
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        None
    }

    fn subtract_scalar(&self) -> Option<&dyn SubtractScalarFn> {
        None
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        None
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        None
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        None
    }
}
