// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! PCO probes retain one decoded page and index the compacted, non-null value positions.

use std::ops::Range;

use pco::data_types::Number;
use pco::data_types::NumberType;
use pco::match_number_enum;
use pco::wrapped::FileDecompressor;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::half;
use vortex_array::match_each_native_ptype;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::Pco;
use crate::PcoArrayExt;
use crate::array::number_type_from_ptype;
use crate::array::vortex_err_from_pco;

const RANK_STRIDE: usize = 512;

/// State retained by repeated PCO probes. Construction performs no allocation.
///
/// The mask and rank index use unsliced logical rows; page boundaries use compacted value
/// positions. Only the most recently accessed page is retained, bounding decoded storage.
#[derive(Default)]
pub struct PcoProbeState {
    validity: Option<Mask>,
    rank: Vec<usize>,
    pages: Vec<Page>,
    decoded: Option<(Range<usize>, PrimitiveArray)>,
    #[cfg(test)]
    decoded_pages: usize,
    #[cfg(test)]
    tracking: tests::TrackedProbeState,
}

struct Page {
    values: Range<usize>,
    chunk: usize,
}

pub(crate) fn scalar_at(
    array: ArrayView<'_, Pco>,
    index: usize,
    state: &mut PcoProbeState,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    let mask = match &mut state.validity {
        Some(mask) => mask,
        slot @ None => slot.insert(
            array
                .unsliced_validity()
                .execute_mask(array.unsliced_n_rows(), ctx)?,
        ),
    };
    let logical_index = array.slice_start() + index;
    if !mask.value(logical_index) {
        return Ok(Scalar::null(array.dtype().clone()));
    }

    let value_index = match mask {
        Mask::AllTrue(_) => logical_index,
        Mask::AllFalse(_) => unreachable!("null rows returned above"),
        Mask::Values(values) => {
            let bits = values.bit_buffer();
            if state.rank.is_empty() {
                state.rank.reserve(bits.len().div_ceil(RANK_STRIDE));
                let mut count = 0;
                for start in (0..bits.len()).step_by(RANK_STRIDE) {
                    state.rank.push(count);
                    count += bits.count_range(start, (start + RANK_STRIDE).min(bits.len()));
                }
            }
            let block = logical_index / RANK_STRIDE;
            state.rank[block] + bits.count_range(block * RANK_STRIDE, logical_index)
        }
    };

    let (range, values) = match &state.decoded {
        Some(decoded) if decoded.0.contains(&value_index) => decoded,
        _ => {
            if state.pages.is_empty() {
                let mut start = 0;
                for (chunk, metadata) in array.metadata.chunks.iter().enumerate() {
                    for page in &metadata.pages {
                        let end = start + page.n_values as usize;
                        state.pages.push(Page {
                            values: start..end,
                            chunk,
                        });
                        start = end;
                    }
                }
            }
            let page_index = state
                .pages
                .partition_point(|page| page.values.end <= value_index);
            let page = state
                .pages
                .get(page_index)
                .ok_or_else(|| vortex_err!("Missing PCO page for value {value_index}"))?;
            let decoded = match_number_enum!(
                number_type_from_ptype(array.dtype().as_ptype()),
                NumberType<T> => { decode_page::<T>(array, page, array.pages[page_index].as_slice(), ctx)? }
            );
            #[cfg(test)]
            {
                state.decoded_pages += 1;
                state.tracking.record_decode();
            }
            state.decoded.insert((page.values.clone(), decoded))
        }
    };
    Ok(match_each_native_ptype!(values.ptype(), |T| {
        Scalar::primitive(
            values.as_slice::<T>()[value_index - range.start],
            array.dtype().nullability(),
        )
    }))
}

fn decode_page<T: Number + NativePType>(
    array: ArrayView<'_, Pco>,
    page: &Page,
    buffer: &[u8],
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    let (file, _) =
        FileDecompressor::new(array.metadata.header.as_slice()).map_err(vortex_err_from_pco)?;
    let (mut chunk, _) = file
        .chunk_decompressor::<T, _>(array.chunk_metas[page.chunk].as_ref())
        .map_err(vortex_err_from_pco)?;
    let mut decoder = chunk
        .page_decompressor(buffer, page.values.len())
        .map_err(vortex_err_from_pco)?;
    let mut values = BufferMut::<T>::zeroed_in(page.values.len(), ctx.allocator().clone());
    decoder.read(&mut values).map_err(vortex_err_from_pco)?;
    Ok(PrimitiveArray::new(values.freeze(), Validity::NonNullable))
}

#[cfg(test)]
mod tests;
