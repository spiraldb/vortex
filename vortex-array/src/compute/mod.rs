//! Compute kernels on top of Vortex Arrays.
//!
//! We aim to provide a basic set of compute kernels that can be used to efficiently index, slice,
//! and filter Vortex Arrays in their encoded forms.
//!
//! Every [array variant][crate::ArrayTrait] has the ability to implement their own efficient
//! implementations of these operators, else we will decode, and perform the equivalent operator
//! from Arrow.

pub use boolean::{and, or, AndFn, OrFn};
pub use compare::{compare, scalar_cmp, CompareFn, MaybeCompareFn, Operator};
pub use filter::{filter, FilterFn};
pub use search_sorted::*;
pub use slice::{slice, SliceFn};
pub use take::{take, TakeFn};
use unary::{CastFn, FillForwardFn, ScalarAtFn, SubtractScalarFn};
use vortex_error::VortexResult;

use crate::Array;

mod boolean;
mod compare;
mod filter;
mod search_sorted;
mod slice;
mod take;

pub mod unary;

/// Trait providing compute functions on top of Vortex arrays.
pub trait ArrayCompute {
    /// Implemented for arrays that can be casted to different types.
    ///
    /// See: [CastFn].
    fn cast(&self) -> Option<&dyn CastFn> {
        None
    }

    /// Binary operator implementation for arrays against other arrays.
    ///
    ///See: [CompareFn].
    fn compare(&self, _array: &Array, _operator: Operator) -> Option<VortexResult<Array>> {
        None
    }

    /// Array function that returns new arrays a non-null value is repeated across runs of nulls.
    ///
    /// See: [FillForwardFn].
    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        None
    }

    /// Filtering function on arrays of predicates.
    ///
    /// See: [FilterFn].
    fn filter(&self) -> Option<&dyn FilterFn> {
        None
    }

    /// Single item indexing on Vortex arrays.
    ///
    /// See: [ScalarAtFn].
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        None
    }

    /// Broadcast subtraction of scalar from Vortex array.
    ///
    /// See: [SubtractScalarFn].
    fn subtract_scalar(&self) -> Option<&dyn SubtractScalarFn> {
        None
    }

    /// Perform a search over an ordered array.
    ///
    /// See: [SearchSortedFn].
    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        None
    }

    /// Perform zero-copy slicing of an array.
    ///
    /// See: [SliceFn].
    fn slice(&self) -> Option<&dyn SliceFn> {
        None
    }

    /// Take a set of indices from an array. This often forces allocations and decoding of
    /// the receiver.
    ///
    /// See: [TakeFn].
    fn take(&self) -> Option<&dyn TakeFn> {
        None
    }

    /// Perform a boolean AND operation over two arrays
    ///
    /// See: [AndFn].
    fn and(&self) -> Option<&dyn AndFn> {
        None
    }

    /// Perform a boolean OR operation over two arrays
    ///
    /// See: [OrFn].
    fn or(&self) -> Option<&dyn OrFn> {
        None
    }
}
