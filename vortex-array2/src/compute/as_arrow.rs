use arrow_array::ArrayRef as ArrowArrayRef;
use vortex_error::{vortex_err, VortexResult};

use crate::compute::flatten::flatten;
use crate::{Array, IntoArray, WithArray};

pub trait AsArrowArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef>;
}

pub fn as_arrow(array: &Array) -> VortexResult<ArrowArrayRef> {
    array.with_array(|a| {
        // If as_arrow is implemented, then invoke that.
        if let Some(a) = a.as_arrow() {
            return a.as_arrow();
        }

        // Otherwise, flatten and try again.
        let array = flatten(array)?.into_array();
        a.as_arrow().map(|a| a.as_arrow()).unwrap_or_else(|| {
            Err(vortex_err!(NotImplemented: "as_arrow", array.encoding().id().name()))
        })
    })
}

// TODO(ngates): return a RecordBatchReader instead?
pub fn as_arrow_chunks(_array: &Array) -> VortexResult<Vec<ArrowArrayRef>> {
    todo!("PORT")
    // if let Some(chunked) = array.as_data::<ChunkedDef>() {
    //     chunked
    //         .chunks()
    //         .iter()
    //         .map(|a| as_arrow(a.as_ref()))
    //         .try_collect()
    // } else {
    //     as_arrow(array).map(|a| vec![a])
    // }
}
