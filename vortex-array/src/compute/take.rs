use log::info;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::{Array, ArrayDType, IntoCanonical};

pub trait TakeFn {
    fn take(&self, indices: &Array) -> VortexResult<Array>;
}

pub fn take(array: &Array, indices: &Array) -> VortexResult<Array> {
    array.with_dyn(|a| {
        if let Some(take) = a.take() {
            if !indices.dtype().is_int() {
                vortex_bail!(InvalidArgument: "indices: expected int or uint array, but found: {}", indices.dtype().python_repr());
            }

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
