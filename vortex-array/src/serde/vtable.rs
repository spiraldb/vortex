use crate::array::{Array, ArrayRef};
use crate::serde::ArrayView;
use vortex_error::VortexResult;

// A VTable for the ArrayData and ArrayView implementations
pub trait VTable<A>: ComputeVTable<A> + Send + Sync {
    fn len(&self, array: &A) -> usize;

    fn to_array(&self, array: &A) -> ArrayRef;

    fn compute(&self) -> &dyn ComputeVTable<A>;

    fn validate(&self, array: &A) -> VortexResult<()>;
}

pub type ArrayViewVTable<'view> = dyn VTable<ArrayView<'view>>;
pub trait ComputeVTable<A> {
    fn take(&self) -> Option<&dyn TakeFn<A>>;
}

pub trait TakeFn<A> {
    fn take(&self, array: &A, indices: &dyn Array) -> VortexResult<ArrayRef>;
}
