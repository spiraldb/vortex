use log::info;
use vortex_error::{vortex_err, VortexResult};

use crate::{Array, IntoCanonical};

pub trait TakeFn {
    fn take(&self, indices: &Array) -> VortexResult<Array>;
}

pub fn take(array: impl AsRef<Array>, indices: impl AsRef<Array>) -> VortexResult<Array> {
    let array = array.as_ref();
    let indices = indices.as_ref();
    array.with_dyn(|a| {
        if let Some(take) = a.take() {
            return take.take(indices);
        }

        // Otherwise, flatten and try again.
        info!("TakeFn not implemented for {}, flattening", array);
        Array::from(array.clone().into_canonical()?).with_dyn(|a| {
            a.take()
                .map(|t| t.take(indices))
                .unwrap_or_else(|| Err(vortex_err!(NotImplemented: "take", array.encoding().id())))
        })
    })
}
