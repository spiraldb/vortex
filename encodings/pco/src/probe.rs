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
}

pub(crate) fn scalar_at(
    array: ArrayView<'_, Pco>,
    index: usize,
    state: Option<&mut PcoProbeState>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    let Some(state) = state else {
        return scalar_at_once(array, index, ctx);
    };
    if state.access.validated_all_null() {
        vortex_ensure!(
            index < array.len(),
            "Pco scalar index exceeds visible array bounds"
        );
        return Ok(Scalar::null(array.dtype().clone()));
    }
    let row = array.slice_start() + index;
    if let Some(value) = state.access.cached_scalar(array, row) {
        return Ok(value);
    }
    scalar_at_uncached(array, row, state, ctx)
}

// Keep the large temporary cache out of the retained-probe hit frame. Stateless
// probes and cache misses are ordinary workloads, so neither helper is cold.
#[inline(never)]
fn scalar_at_once(
    array: ArrayView<'_, Pco>,
    index: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    scalar_at(array, index, Some(&mut PcoProbeState::default()), ctx)
}

#[inline(never)]
fn scalar_at_uncached(
    array: ArrayView<'_, Pco>,
    row: usize,
    state: &mut PcoProbeState,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    let Some((page, value)) = state.access.resolve(array, row, ctx)? else {
        return Ok(Scalar::null(array.dtype().clone()));
    };
    let values = state.access.values(array, page, ctx)?;
    Ok(match_each_native_ptype!(values.ptype(), |T| {
        Scalar::primitive(values.as_slice::<T>()[value], array.dtype().nullability())
    }))
}

#[cfg(test)]
mod tests;
