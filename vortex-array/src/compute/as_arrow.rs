use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::Array;
use crate::compute::flatten::flatten;
use crate::error::{VortexError, VortexResult};
use arrow_array::ArrayRef as ArrowArrayRef;
use itertools::Itertools;

pub trait AsArrowArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef>;
}

pub fn as_arrow(array: &dyn Array) -> VortexResult<ArrowArrayRef> {
    // If as_arrow is implemented, then invoke that.
    if let Some(a) = array.as_arrow() {
        return a.as_arrow();
    }

    // Otherwise, flatten and try again.
    let array = flatten(array)?.into_array();
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

#[cfg(test)]
mod tests {
    use crate::array::chunked::ChunkedArray;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Array;
    use crate::compute::as_arrow::as_arrow;

    #[test]
    fn test_chunked() {
        let c1 = PrimitiveArray::from(vec![1i64, 2, 3]);
        let c2 = PrimitiveArray::from(vec![4i64, 5, 6]);
        let array = ChunkedArray::new(vec![c1.clone().boxed(), c2.boxed()], c1.dtype().clone());

        as_arrow(array.as_ref()).unwrap();
    }
}
