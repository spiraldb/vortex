use vortex_error::{vortex_err, VortexResult};

use crate::Array;

pub trait FillForwardFn {
    fn fill_forward(&self) -> VortexResult<Array>;
}

pub fn fill_forward(array: &Array) -> VortexResult<Array<'static>> {
    if !array.dtype().is_nullable() {
        return Ok(array.to_array());
    }

    array.with_compute(|c| {
        c.fill_forward()
            .map(|t| t.fill_forward())
            .unwrap_or_else(|| {
                Err(vortex_err!(
                    NotImplemented: "fill_forward",
                    array.encoding().id().name()
                ))
            })
    })
}
