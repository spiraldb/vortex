use log::info;

use vortex_error::{vortex_err, VortexResult};

use crate::array::{Array, ArrayRef, WithArrayCompute};
use crate::compute::flatten::flatten;

pub trait TakeFn {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef>;
}

pub fn take(array: &dyn Array, indices: &dyn Array) -> VortexResult<ArrayRef> {
    array.with_compute(|c| {
        if let Some(take) = c.take() {
            return take.take(indices);
        }

        // Otherwise, flatten and try again.
        info!("TakeFn not implemented for {}, flattening", array);
        flatten(array)?.into_array().with_compute(|c| {
            c.take().map(|t| t.take(indices)).unwrap_or_else(|| {
                Err(vortex_err!(NotImplemented: "take", array.encoding().id().name()))
            })
        })
    })
}
