use crate::array::Array;
use crate::compute::flatten::flatten;
use crate::error::{VortexError, VortexResult};
use arrow_array::ArrayRef as ArrowArrayRef;

pub trait AsArrowArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef>;
}

pub fn as_arrow(array: &dyn Array) -> VortexResult<ArrowArrayRef> {
    // First we flatten the array
    flatten(array)?
        .as_arrow()
        .map(|a| a.as_arrow())
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "as_arrow",
                array.encoding().id(),
            ))
        })
}
