use crate::array::Array;
use crate::error::{VortexError, VortexResult};
use crate::scalar::ScalarRef;

pub trait ScalarAtFn {
    fn scalar_at(&self, index: usize) -> VortexResult<ScalarRef>;
}

pub trait UsizeAtFn {
    fn usize_at(&self, index: usize) -> VortexResult<usize>;
}

pub fn scalar_at(array: &dyn Array, index: usize) -> VortexResult<ScalarRef> {
    if index >= array.len() {
        return Err(VortexError::OutOfBounds(index, 0, array.len()));
    }

    array
        .scalar_at()
        .map(|t| t.scalar_at(index))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "scalar_at",
                array.encoding().id(),
            ))
        })
}

pub fn usize_at(array: &dyn Array, index: usize) -> VortexResult<usize> {
    if index >= array.len() {
        return Err(VortexError::OutOfBounds(index, 0, array.len()));
    }

    array
        .usize_at()
        .map(|t| t.usize_at(index))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "usize_at",
                array.encoding().id(),
            ))
        })
}
