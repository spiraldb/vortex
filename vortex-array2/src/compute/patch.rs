use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::{Array, ArrayTrait};

pub trait PatchFn {
    fn patch(&self, patch: &Array) -> VortexResult<Array>;
}

/// Returns a new array where the non-null values from the patch array are replaced in the original.
pub fn patch(array: &Array, patch: &Array) -> VortexResult<Array<'static>> {
    if array.len() != patch.len() {
        vortex_bail!(
            "patch array {} must have the same length as the original array {}",
            patch,
            array
        );
    }

    if array.dtype().as_nullable() != patch.dtype().as_nullable() {
        vortex_bail!(MismatchedTypes: array.dtype(), patch.dtype());
    }

    array.with_compute(|c| {
        c.patch().map(|t| t.patch(patch)).unwrap_or_else(|| {
            Err(vortex_err!(NotImplemented: "take", array.encoding().id().name()))
        })
    })
}
