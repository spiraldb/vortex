use vortex_error::{VortexError, VortexResult};

use crate::array::{Array, ArrayRef, WithArrayCompute};

pub trait FillForwardFn {
    fn fill_forward(&self) -> VortexResult<ArrayRef>;
}

pub fn fill_forward(array: &dyn Array) -> VortexResult<ArrayRef> {
    if !array.dtype().is_nullable() {
        return Ok(array.to_array());
    }

    array.with_compute(|c| {
        c.fill_forward()
            .map(|t| t.fill_forward())
            .unwrap_or_else(|| {
                Err(VortexError::NotImplemented(
                    "fill_forward",
                    array.encoding().id().name(),
                ))
            })
    })
}
