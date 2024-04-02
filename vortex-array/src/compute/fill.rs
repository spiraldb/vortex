use vortex_error::{vortex_err, VortexResult};

use crate::array::{Array, ArrayRef};

pub trait FillForwardFn {
    fn fill_forward(&self) -> VortexResult<ArrayRef>;
}

pub fn fill_forward(array: &dyn Array) -> VortexResult<ArrayRef> {
    if !array.dtype().is_nullable() {
        return Ok(array.to_array());
    }

    array
        .fill_forward()
        .map(|t| t.fill_forward())
        .unwrap_or_else(|| {
            Err(vortex_err!(
                NotImplemented: "fill_forward",
                array.encoding().id().name()
            ))
        })
}
