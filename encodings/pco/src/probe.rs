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

/// State retained by repeated Pco probes: the row-to-value mapping and the most recently decoded
/// block. Construction performs no allocation.
#[derive(Default)]
pub struct PcoProbeState {
    rows: Option<RowValues>,
    block: Option<DecodedBlock>,
}

struct DecodedBlock {
    /// Array-wide index of the block.
    index: usize,
    /// Positions among the valid values that the block covers.
    values: Range<usize>,
    decoded: PrimitiveArray,
}

pub(crate) fn scalar_at(
    array: ArrayView<'_, Pco>,
    index: usize,
    state: Option<&mut PcoProbeState>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    let row = array.slice_start() + index;
    let mut once = PcoProbeState::default();
    let retained = state.is_some();
    let (value, state) = match state {
        Some(state) => {
            let rows = match &mut state.rows {
                Some(rows) => rows,
                slot @ None => slot.insert(RowValues::new(
                    array
                        .unsliced_validity()
                        .execute_mask(array.unsliced_n_rows(), ctx)?,
                )),
            };
            (rows.value_index(row), state)
        }
        // A one-off read needs only this row's rank, not a rank index over every row.
        None => {
            let value = match array
                .unsliced_validity()
                .execute_mask(array.unsliced_n_rows(), ctx)?
            {
                Mask::AllTrue(_) => Some(row),
                Mask::AllFalse(_) => None,
                Mask::Values(values) => values
                    .value(row)
                    .then(|| values.bit_buffer().count_range(0, row)),
            };
            (value, &mut once)
        }
    };
    let Some(value) = value else {
        return Ok(Scalar::null(array.dtype().clone()));
    };

    let page = array.page_of(value);
    let block = array.block_of(page, value);
    if state
        .block
        .as_ref()
        .is_none_or(|cached| cached.index != block)
    {
        let entry = &array.entries[page];
        let local = block - entry.first_block;
        let mut page_values = entry.block_values(local..local + 1);
        if !retained {
            // A one-off read never reuses the block, so stop at the requested value.
            page_values.end = value - entry.values.start + 1;
        }
        let values = entry.values.start + page_values.start..entry.values.start + page_values.end;
        let decoded = match_number_enum!(
            number_type_from_ptype(array.ptype()),
            NumberType<T> => {
                let mut decoded = BufferMut::<T>::with_capacity(values.len());
                BlockDecoder::<T>::new(array.data(), true)?
                    .append_page_values(page, page_values, &mut decoded)?;
                PrimitiveArray::new(decoded.freeze(), Validity::NonNullable)
            }
        );
        state.block = Some(DecodedBlock {
            index: block,
            values,
            decoded,
        });
    }

    let Some(cached) = &state.block else {
        unreachable!("the block was just decoded");
    };
    Ok(match_each_native_ptype!(cached.decoded.ptype(), |T| {
        Scalar::primitive(
            cached.decoded.as_slice::<T>()[value - cached.values.start],
            array.dtype().nullability(),
        )
    }))
}

#[cfg(test)]
mod tests;
