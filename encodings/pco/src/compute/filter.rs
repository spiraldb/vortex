// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::filter::FilterKernel;
use vortex_error::VortexResult;
use vortex_mask::Mask;

use super::rows::NO_VALUE;
use super::rows::decode_runs_array;
use super::rows::gather_array;
use super::rows::row_values;
use crate::Pco;

/// Decodes only the blocks holding selected rows, and declines when that is most of them.
impl FilterKernel for Pco {
    fn filter(
        array: ArrayView<'_, Self>,
        mask: &Mask,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        let Some(mask_values) = mask.values() else {
            return Ok(None);
        };
        let data = array.data();
        let rows = row_values(array, ctx)?;
        let slice_start = array.slice_start();
        let block_of = |value: usize| data.block_of(data.page_of(value), value);

        // When the selection touches most blocks, decoding everything and filtering the
        // primitive array beats gathering value by value.
        let all_values = rows.rank(slice_start)..rows.rank(array.slice_stop());
        if all_values.is_empty() {
            return Ok(None);
        }
        let n_blocks = block_of(all_values.end - 1) - block_of(all_values.start) + 1;
        let mut touched_blocks = 0;
        let mut last_block = None;
        for &(start, end) in mask_values.slices() {
            let values = rows.rank(slice_start + start)..rows.rank(slice_start + end);
            if values.is_empty() {
                continue;
            }
            let first = block_of(values.start);
            let last = block_of(values.end - 1);
            touched_blocks += last - first + 1 - usize::from(last_block == Some(first));
            last_block = Some(last);
        }
        if 2 * touched_blocks > n_blocks {
            return Ok(None);
        }
        let validity = array.validity()?.filter(mask)?;

        // Runs that rarely share a block decode fastest as contiguous ranges.
        if mask_values.slices().len() <= touched_blocks {
            return decode_runs_array(
                data,
                &rows,
                slice_start,
                mask_values.slices(),
                mask_values.true_count(),
                validity,
                ctx,
            )
            .map(Some);
        }

        let positions = mask_values
            .indices()
            .iter()
            .map(|&index| rows.value_index(slice_start + index).unwrap_or(NO_VALUE))
            .collect::<Vec<_>>();
        gather_array(data, &positions, validity, ctx).map(Some)
    }
}
