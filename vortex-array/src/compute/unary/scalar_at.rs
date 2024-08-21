use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::{Array, ArrayDType};

pub trait ScalarAtFn {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar>;

    fn scalar_at_unchecked(&self, index: usize) -> Scalar;
}

pub fn scalar_at(array: &Array, index: usize) -> VortexResult<Scalar> {
    if index >= array.len() {
        vortex_bail!(OutOfBounds: index, 0, array.len());
    }

    if !array.with_dyn(|a| a.is_valid(index)) {
        return Ok(Scalar::null(array.dtype().clone()));
    }

    array.with_dyn(|a| {
        a.scalar_at()
            .map(|t| t.scalar_at(index))
            .unwrap_or_else(|| Err(vortex_err!(NotImplemented: "scalar_at", array.encoding().id())))
    })
}

pub fn scalar_at_unchecked(array: &Array, index: usize) -> Scalar {
    array
        .with_dyn(|a| a.scalar_at().map(|s| s.scalar_at_unchecked(index)))
        .ok_or_else(|| vortex_err!(NotImplemented: "scalar_at", array.encoding().id()))
        .unwrap()
}
