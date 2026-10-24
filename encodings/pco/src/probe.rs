// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! PCO probes retain every decoded page and index the compacted, non-null value positions.

use pco::data_types::Number;
use pco::wrapped::FileDecompressor;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::NativePType;
use vortex_array::match_each_native_ptype;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;

use crate::Pco;
use crate::access::AccessState;
use crate::array::vortex_err_from_pco;

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
    let mut once = PcoProbeState::default();
    let state = state.unwrap_or(&mut once);
    let row = array.slice_start() + index;
    let Some((page, value)) = state.access.resolve(array, row, ctx)? else {
        return Ok(Scalar::null(array.dtype().clone()));
    };
    let values = state.access.values(array, page, ctx)?;
    Ok(match_each_native_ptype!(values.ptype(), |T| {
        Scalar::primitive(values.as_slice::<T>()[value], array.dtype().nullability())
    }))
}

pub(crate) fn decode_page<T: Number + NativePType>(
    array: ArrayView<'_, Pco>,
    chunk: usize,
    n_values: usize,
    buffer: &[u8],
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    let (file, _) =
        FileDecompressor::new(array.metadata.header.as_slice()).map_err(vortex_err_from_pco)?;
    let (mut chunk, _) = file
        .chunk_decompressor::<T, _>(array.chunk_metas[chunk].as_ref())
        .map_err(vortex_err_from_pco)?;
    let mut decoder = chunk
        .page_decompressor(buffer, n_values)
        .map_err(vortex_err_from_pco)?;
    let mut values = BufferMut::<T>::with_capacity_in(n_values, ctx.allocator().clone());
    // SAFETY: the buffer reserves `n_values` elements, and the page decompressor was built for
    // exactly that count, so `read` writes every element before anything observes it.
    unsafe { values.set_len(n_values) };
    decoder.read(&mut values).map_err(vortex_err_from_pco)?;
    Ok(PrimitiveArray::new(values.freeze(), Validity::NonNullable))
}

#[cfg(test)]
mod tests;
