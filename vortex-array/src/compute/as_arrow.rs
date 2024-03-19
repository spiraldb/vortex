use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::Array;
use crate::error::{VortexError, VortexResult};
use arrow_array::ArrayRef as ArrowArrayRef;
use itertools::Itertools;

pub trait AsArrowArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef>;
}

pub fn as_arrow(array: &dyn Array) -> VortexResult<ArrowArrayRef> {
    array.as_arrow().map(|a| a.as_arrow()).unwrap_or_else(|| {
        Err(VortexError::NotImplemented(
            "as_arrow",
            array.encoding().id(),
        ))
    })
}

// TODO(ngates): return a RecordBatchReader instead?
pub fn as_arrow_chunks(array: &dyn Array) -> VortexResult<Vec<ArrowArrayRef>> {
    if let Some(chunked) = array.maybe_chunked() {
        chunked
            .chunks()
            .iter()
            .map(|a| as_arrow(a.as_ref()))
            .try_collect()
    } else {
        as_arrow(array).map(|a| vec![a])
    }
}
