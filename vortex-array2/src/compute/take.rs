use log::info;
use vortex_error::{vortex_err, VortexResult};

use crate::compute::flatten::flatten;
use crate::{Array, IntoArray, OwnedArray, WithArray};

pub trait TakeFn {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray>;
}

pub fn take(array: &Array, indices: &Array) -> VortexResult<OwnedArray> {
    array.with_array(|a| {
        if let Some(take) = a.take() {
            return take.take(indices);
        }

        // Otherwise, flatten and try again.
        info!("TakeFn not implemented for {}, flattening", array);
        flatten(array)?.into_array().with_array(|a| {
            a.take().map(|t| t.take(indices)).unwrap_or_else(|| {
                Err(vortex_err!(NotImplemented: "take", array.encoding().id().name()))
            })
        })
    })
}
