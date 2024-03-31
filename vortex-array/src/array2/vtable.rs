use crate::array2::{ArrayData, ArrayView};
use vortex_error::VortexResult;

// A VTable for the ArrayData and ArrayView implementations
pub trait VTable<A>: Send + Sync {
    fn len(&self, array: &A) -> usize;

    fn validate(&self, array: &A) -> VortexResult<()>;
}

pub type ArrayViewVTable<'view> = dyn VTable<ArrayView<'view>>;
pub type ArrayDataVTable = dyn VTable<ArrayData>;
