use crate::array::Array;
use crate::compute::flatten::flatten;
use crate::error::VortexResult;
use arrow_array::ArrayRef as ArrowArrayRef;

pub trait AsArrowArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef>;
}

pub fn as_arrow(array: &dyn Array) -> VortexResult<ArrowArrayRef> {
    flatten(array)?.as_arrow()
}
