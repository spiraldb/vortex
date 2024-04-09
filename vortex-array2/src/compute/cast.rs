use vortex_error::{vortex_err, VortexResult};
use vortex_schema::DType;

use crate::Array;

pub trait CastFn {
    fn cast(&self, dtype: &DType) -> VortexResult<Array>;
}

pub fn cast(array: &Array, dtype: &DType) -> VortexResult<Array<'static>> {
    if array.dtype() == dtype {
        return Ok(array.to_array());
    }

    // TODO(ngates): check for null_count if dtype is non-nullable
    array.with_compute(|c| {
        c.cast().map(|f| f.cast(dtype)).unwrap_or_else(|| {
            Err(vortex_err!(NotImplemented: "cast", array.encoding().id().name()))
        })
    })
}
