use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::array::Array;
use crate::scalar::Scalar;

pub trait ScalarAtFn {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar>;
}

pub fn scalar_at(array: &dyn Array, index: usize) -> VortexResult<Scalar> {
    if index >= array.len() {
        vortex_bail!(OutOfBounds: index, 0, array.len());
    }

    array
        .scalar_at()
        .map(|t| t.scalar_at(index))
        .unwrap_or_else(|| {
            Err(vortex_err!(NotImplemented: "scalar_at", array.encoding().id().name()))
        })
}
