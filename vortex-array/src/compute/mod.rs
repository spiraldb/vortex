//! Compute kernels on top of Vortex Arrays.
//!
//! We aim to provide a basic set of compute kernels that can be used to efficiently index, slice,
//! and filter Vortex Arrays in their encoded forms.
//!
//! Every [array variant][crate::ArrayTrait] has the ability to implement their own efficient
//! implementations of these operators, else we will decode, and perform the equivalent operator
//! from Arrow.

use compare::CompareFn;
use search_sorted::SearchSortedFn;
use slice::SliceFn;
use take::TakeFn;

use self::filter_indices::FilterIndicesFn;
use self::unary::cast::CastFn;
use self::unary::fill_forward::FillForwardFn;
use self::unary::scalar_at::ScalarAtFn;
use self::unary::scalar_subtract::SubtractScalarFn;
use crate::compute::filter::FilterFn;

pub mod compare;
mod filter;
pub mod filter_indices;
pub mod search_sorted;
pub mod slice;
pub mod take;
pub mod unary;

/// Trait providing compute functions on top of
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

    fn filter(&self) -> Option<&dyn FilterFn> {
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
