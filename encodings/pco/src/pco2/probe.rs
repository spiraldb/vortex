// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Pco2 probes decode only the checkpoint block holding the requested row.

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

use super::BlockDecoder;
use super::Pco2;
use super::Pco2ArrayExt;
use super::compute::RowValues;
use crate::array::number_type_from_ptype;

/// State retained by repeated Pco2 probes: the row-to-value mapping and the most recently
/// decoded checkpoint block. Construction performs no allocation.
#[derive(Default)]
pub struct Pco2ProbeState {
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
    array: ArrayView<'_, Pco2>,
    index: usize,
    state: Option<&mut Pco2ProbeState>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Scalar> {
    let row = array.slice_start() + index;
    let mut once = Pco2ProbeState::default();
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
        let entry = &array.index[page];
        let local = block - entry.first_block;
        let interval = array.interval();
        let values = entry.values.start + local * interval
            ..(entry.values.start + (local + 1) * interval).min(entry.values.end);
        let decoded = match_number_enum!(
            number_type_from_ptype(array.ptype()),
            NumberType<T> => {
                let mut decoded = BufferMut::<T>::with_capacity(values.len());
                BlockDecoder::<T>::new(array.data())?
                    .append_page_blocks(page, local..local + 1, &mut decoded)?;
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
