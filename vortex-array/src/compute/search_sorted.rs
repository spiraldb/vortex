use crate::array::Array;
use crate::error::{VortexError, VortexResult};
use crate::scalar::{Scalar, ScalarRef};

pub enum SearchSortedSide {
    Left,
    Right,
}

pub trait SearchSortedFn {
    fn search_sorted(&self, value: &dyn Scalar, side: SearchSortedSide) -> VortexResult<usize>;
}

pub fn search_sorted<T: Into<ScalarRef>>(
    array: &dyn Array,
    target: T,
    side: SearchSortedSide,
) -> VortexResult<usize> {
    let scalar = target.into().cast(array.dtype())?;
    array
        .search_sorted()
        .map(|f| f.search_sorted(scalar.as_ref(), side))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "search_sorted",
                array.encoding().id(),
            ))
        })
}
