// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! RunEnd probes retain their routing and value child probes across lookups.

use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::ProbeCtx;
use vortex_array::scalar::Scalar;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use crate::RunEnd;
use crate::RunEndArrayExt;
use crate::RunEndSlots;

pub(crate) fn scalar_at<'a>(
    array: ArrayView<'a, RunEnd>,
    index: usize,
    probe: &mut ProbeCtx<'a, ()>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    let logical_index = array
        .offset()
        .checked_add(index)
        .ok_or_else(|| vortex_err!("RunEnd logical index overflow"))?;
    // Search for the first end strictly greater than the logical index. Every comparison
    // uses the same ends probe, preserving child preparation within and between searches.
    let ends = probe.child(RunEndSlots::ENDS)?;
    let mut left = 0;
    let mut right = ends.array().len();
    while left < right {
        let mid = left + (right - left) / 2;
        let end = usize::try_from(&ends.scalar_at(mid, ctx)?)?;
        if end <= logical_index {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    // The selected value supplies nullness too; probing expanded RunEnd validity would
    // repeat the search and lose the values child's retained state.
    probe.child(RunEndSlots::VALUES)?.scalar_at(left, ctx)
}

#[cfg(test)]
mod tests;
