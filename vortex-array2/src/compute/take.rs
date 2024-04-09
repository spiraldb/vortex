use log::info;
use vortex_error::{vortex_err, VortexResult};

use crate::compute::flatten::flatten;
use crate::Array;

pub trait TakeFn {
    fn take(&self, indices: &Array) -> VortexResult<Array>;
}

pub fn take(array: &Array, indices: &Array) -> VortexResult<Array<'static>> {
    array.with_compute(|c| {
        if let Some(take) = c.take() {
            return take.take(indices);
        }

        // Otherwise, flatten and try again.
        info!("TakeFn not implemented for {}, flattening", array);
        flatten(array)?.to_array_data().with_compute(|c| {
            c.take().map(|t| t.take(indices)).unwrap_or_else(|| {
                Err(vortex_err!(NotImplemented: "take", array.encoding().id().name()))
            })
        })
    })
}
