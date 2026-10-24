// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! PCO probes retain every decoded page and index the compacted, non-null value positions.

use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::match_each_native_ptype;
use vortex_array::scalar::Scalar;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;

use crate::Pco;
use crate::access::AccessState;

/// State retained by repeated PCO probes. Construction performs no allocation.
///
/// Decoded pages and their local validity are retained as touched, up to the full array.
/// Legacy files additionally retain a full validity mask and its sparse rank index.
#[derive(Default)]
pub struct PcoProbeState {
    access: AccessState,
    all_null: Option<Scalar>,
}

pub(crate) fn scalar_at(
    array: ArrayView<'_, Pco>,
    index: usize,
    state: Option<&mut PcoProbeState>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    // Retained probes must not construct/drop a fresh fallback cache on every hit.
    let mut once;
    let state = match state {
        Some(state) => state,
        None => {
            once = PcoProbeState::default();
            &mut once
        }
    };
    if let Some(null) = &state.all_null {
        vortex_ensure!(
            index < array.len(),
            "Pco scalar index exceeds visible array bounds"
        );
        return Ok(null.clone());
    }
    let row = array.slice_start() + index;
    if let Some(value) = state.access.cached_scalar(array, row) {
        return Ok(value);
    }
    let Some((page, value)) = state.access.resolve(array, row, ctx)? else {
        let null = Scalar::null(array.dtype().clone());
        if state.access.validated_all_null() {
            state.all_null = Some(null.clone());
        }
        return Ok(null);
    };
    let values = state.access.values(array, page, ctx)?;
    Ok(match_each_native_ptype!(values.ptype(), |T| {
        Scalar::primitive(values.as_slice::<T>()[value], array.dtype().nullability())
    }))
}

#[cfg(test)]
mod tests;
