// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Pco probes decode only the block holding the requested row: a checkpoint block in v2, or a
//! page in v1.

use std::ops::Range;

use pco::data_types::NumberType;
use pco::match_number_enum;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::half;
use vortex_array::match_each_native_ptype;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_mask::Mask;

use crate::Pco;
use crate::PcoArrayExt;
use crate::array::BlockDecoder;
use crate::array::number_type_from_ptype;
use crate::compute::RowValues;

/// State retained by repeated Pco probes: the row-to-value mapping and every block decoded so
/// far, so the state grows with the blocks touched and never beyond the decompressed array.
/// Construction performs no allocation.
#[derive(Default)]
pub struct PcoProbeState {
    rows: Option<RowValues>,
    /// Decoded blocks by array-wide block index.
    blocks: Vec<Option<PrimitiveArray>>,
}

pub(crate) fn scalar_at(
    array: ArrayView<'_, Pco>,
    index: usize,
    mut state: Option<&mut PcoProbeState>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    let row = array.slice_start() + index;
    let value = match state.as_deref_mut() {
        Some(state) => {
            let rows = match &mut state.rows {
                Some(rows) => rows,
                slot @ None => slot.insert(RowValues::new(
                    array
                        .unsliced_validity()
                        .execute_mask(array.unsliced_n_rows(), ctx)?,
                )),
            };
            rows.value_index(row)
        }
        // A one-off read needs only this row's rank, not a rank index over every row.
        None => match array
            .unsliced_validity()
            .execute_mask(array.unsliced_n_rows(), ctx)?
        {
            Mask::AllTrue(_) => Some(row),
            Mask::AllFalse(_) => None,
            Mask::Values(values) => values
                .value(row)
                .then(|| values.bit_buffer().count_range(0, row)),
        },
    };
    let Some(value) = value else {
        return Ok(Scalar::null(array.dtype().clone()));
    };

    let page = array.page_of(value);
    let block = array.block_of(page, value);
    let entry = &array.entries[page];
    let local = block - entry.first_block;
    let mut page_values = entry.block_values(local..local + 1);
    let offset = value - entry.values.start - page_values.start;
    let nullability = array.dtype().nullability();
    let Some(state) = state else {
        // A one-off read never reuses the block, so stop at the requested value.
        page_values.end = value - entry.values.start + 1;
        let decoded = decode_block(array, page, page_values)?;
        return Ok(scalar_in(&decoded, offset, nullability));
    };
    if state.blocks.is_empty() {
        state.blocks.resize_with(array.n_blocks(), || None);
    }
    let decoded = match &mut state.blocks[block] {
        Some(decoded) => decoded,
        slot @ None => slot.insert(decode_block(array, page, page_values)?),
    };
    Ok(scalar_in(decoded, offset, nullability))
}

/// Decodes values `page_values` of page `page`, numbered within the page.
fn decode_block(
    array: ArrayView<'_, Pco>,
    page: usize,
    page_values: Range<usize>,
) -> VortexResult<PrimitiveArray> {
    Ok(match_number_enum!(
        number_type_from_ptype(array.ptype()),
        NumberType<T> => {
            let mut decoded = BufferMut::<T>::with_capacity(page_values.len());
            BlockDecoder::<T>::new(array.data(), true)?
                .append_page_values(page, page_values, &mut decoded)?;
            PrimitiveArray::new(decoded.freeze(), Validity::NonNullable)
        }
    ))
}

fn scalar_in(decoded: &PrimitiveArray, offset: usize, nullability: Nullability) -> Scalar {
    match_each_native_ptype!(decoded.ptype(), |T| {
        Scalar::primitive(decoded.as_slice::<T>()[offset], nullability)
    })
}

#[cfg(test)]
mod tests;
