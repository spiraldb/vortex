// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Shared machinery for kernels that decode only the blocks an operation touches.

use pco::FULL_BATCH_N;
use pco::data_types::Number;
use pco::data_types::NumberType;
use pco::match_number_enum;
use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::half;
use vortex_array::validity::Validity;
use vortex_buffer::BitBuffer;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::Pco;
use crate::PcoArrayExt;
use crate::PcoData;
use crate::array::BlockDecoder;
use crate::array::number_type_from_ptype;

const RANK_STRIDE: usize = 512;

/// Maps rows of the unsliced array to positions among its valid values.
pub(crate) enum RowValues {
    AllValid,
    AllNull,
    Ranked {
        bits: BitBuffer,
        /// Valid rows before each multiple of [`RANK_STRIDE`], plus the total.
        rank: Vec<usize>,
    },
}

impl RowValues {
    pub(crate) fn new(mask: Mask) -> Self {
        match mask {
            Mask::AllTrue(_) => Self::AllValid,
            Mask::AllFalse(_) => Self::AllNull,
            Mask::Values(values) => {
                let bits = values.bit_buffer().clone();
                let mut rank = Vec::with_capacity(bits.len().div_ceil(RANK_STRIDE) + 1);
                let mut count = 0;
                for start in (0..bits.len()).step_by(RANK_STRIDE) {
                    rank.push(count);
                    count += bits.count_range(start, (start + RANK_STRIDE).min(bits.len()));
                }
                rank.push(count);
                Self::Ranked { bits, rank }
            }
        }
    }

    /// The number of valid rows before `row`.
    pub(crate) fn rank(&self, row: usize) -> usize {
        match self {
            Self::AllValid => row,
            Self::AllNull => 0,
            Self::Ranked { bits, rank } => {
                let block = row / RANK_STRIDE;
                rank[block] + bits.count_range(block * RANK_STRIDE, row)
            }
        }
    }

    /// The position of `row` among the valid values, or `None` if the row is null.
    #[inline]
    pub(crate) fn value_index(&self, row: usize) -> Option<usize> {
        match self {
            Self::AllValid => Some(row),
            Self::AllNull => None,
            Self::Ranked { bits, .. } => bits.value(row).then(|| self.rank(row)),
        }
    }
}

pub(crate) fn row_values(
    array: ArrayView<'_, Pco>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<RowValues> {
    Ok(RowValues::new(
        array
            .unsliced_validity()
            .execute_mask(array.unsliced_n_rows(), ctx)?,
    ))
}

/// Marks an output position that reads no value.
pub(crate) const NO_VALUE: usize = usize::MAX;

/// Reads the valid values at `positions`, or `T::default()` for [`NO_VALUE`], decoding every
/// touched block once, through the batch holding the last value read from it. A block whose last
/// batch is read decodes together with the next touched block when that one follows it.
fn gather<T: Number + NativePType>(
    data: &PcoData,
    positions: &[usize],
    ctx: &mut ExecutionCtx,
) -> VortexResult<Buffer<T>> {
    if data.has_index() {
        return gather_indexed(data, positions, ctx);
    }
    let block_ids = positions
        .iter()
        .map(|&position| {
            if position == NO_VALUE {
                NO_VALUE
            } else {
                data.block_of(data.page_of(position), position)
            }
        })
        .collect::<Vec<_>>();
    let mut touched = block_ids
        .iter()
        .copied()
        .filter(|&block| block != NO_VALUE)
        .collect::<Vec<_>>();
    touched.sort_unstable();
    touched.dedup();
    // The last value read from each touched block.
    let mut last_read = vec![0; touched.len()];
    for (&position, &block) in positions.iter().zip(&block_ids) {
        if block != NO_VALUE {
            let idx = touched
                .binary_search(&block)
                .map_err(|_| vortex_err!("Pco block {block} was not collected"))?;
            last_read[idx] = last_read[idx].max(position);
        }
    }

    let mut decoder = BlockDecoder::<T>::new(data, true)?;
    let mut decoded = BufferMut::<T>::empty();
    // For each touched block, where its values start in `decoded` and among the valid values.
    let mut decoded_starts = Vec::with_capacity(touched.len());
    let mut value_starts = Vec::with_capacity(touched.len());
    let mut run_start = 0;
    while run_start < touched.len() {
        let page = data.page_of_block(touched[run_start]);
        let entry = &data.entries[page];
        let page_blocks = entry.first_block..entry.first_block + entry.n_blocks();
        // Joining a block to the run decodes the previous block to its end, which is free only
        // when its last batch is read anyway.
        let reads_to_end = |idx: usize| {
            let block = touched[idx] - entry.first_block;
            let block_end = entry.values.start + entry.block_values(block..block + 1).end;
            last_read[idx] + FULL_BATCH_N >= block_end
        };
        let mut run_end = run_start + 1;
        while run_end < touched.len()
            && touched[run_end] == touched[run_end - 1] + 1
            && page_blocks.contains(&touched[run_end])
            && reads_to_end(run_end - 1)
        {
            run_end += 1;
        }

        let first_block = touched[run_start] - entry.first_block;
        let run_values = entry.block_values(first_block..first_block + 1).start
            ..last_read[run_end - 1] + 1 - entry.values.start;
        for block in first_block..touched[run_end - 1] + 1 - entry.first_block {
            let block_start = entry.block_values(block..block + 1).start;
            decoded_starts.push(decoded.len() + block_start - run_values.start);
            value_starts.push(entry.values.start + block_start);
        }
        decoder.append_page_values(page, run_values, &mut decoded)?;
        run_start = run_end;
    }

    let mut out = BufferMut::<T>::with_capacity_in(positions.len(), ctx.allocator().clone());
    let mut cursor = 0;
    for (&position, &block) in positions.iter().zip(&block_ids) {
        if position == NO_VALUE {
            out.push(T::default());
            continue;
        }
        if touched[cursor] != block {
            cursor = touched
                .binary_search(&block)
                .map_err(|_| vortex_err!("Pco block {block} was not decoded"))?;
        }
        out.push(decoded[decoded_starts[cursor] + position - value_starts[cursor]]);
    }
    Ok(out.freeze())
}

/// Scatter each decoded run into the result immediately, so checkpoint reads reuse scratch
/// space instead of retaining and repeatedly growing a buffer of all touched blocks.
fn gather_indexed<T: Number + NativePType>(
    data: &PcoData,
    positions: &[usize],
    ctx: &mut ExecutionCtx,
) -> VortexResult<Buffer<T>> {
    let mut ordered = positions
        .iter()
        .copied()
        .enumerate()
        .filter(|&(_, position)| position != NO_VALUE)
        .collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|&(_, position)| position);
    let mut out = BufferMut::<T>::with_capacity_in(positions.len(), ctx.allocator().clone());
    out.push_n(T::default(), positions.len());
    let mut decoder = BlockDecoder::<T>::new(data, true)?;
    let mut decoded = BufferMut::<T>::empty();
    let mut start = 0;
    while start < ordered.len() {
        let first = ordered[start].1;
        let page = data.page_of(first);
        let entry = &data.entries[page];
        let first_block = (first - entry.values.start) / entry.block_len;
        let mut last_block = first_block;
        let mut last = first;
        let mut end = start + 1;
        while end < ordered.len() {
            let next = ordered[end].1;
            if next >= entry.values.end {
                break;
            }
            let block = (next - entry.values.start) / entry.block_len;
            if block != last_block {
                let block_end = entry.values.start
                    + entry.block_values(last_block..last_block + 1).end;
                if block != last_block + 1 || last + FULL_BATCH_N < block_end {
                    break;
                }
            }
            last_block = block;
            last = next;
            end += 1;
        }
        let local_start = first_block * entry.block_len;
        decoded.clear();
        decoder.append_page_values(
            page,
            local_start..last + 1 - entry.values.start,
            &mut decoded,
        )?;
        let value_start = entry.values.start + local_start;
        for &(output, position) in &ordered[start..end] {
            out[output] = decoded[position - value_start];
        }
        start = end;
    }
    Ok(out.freeze())
}

/// Reads the valid values at `positions` as a primitive array with `validity`.
pub(crate) fn gather_array(
    data: &PcoData,
    positions: &[usize],
    validity: Validity,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrayRef> {
    Ok(match_number_enum!(
        number_type_from_ptype(data.ptype()),
        NumberType<T> => {
            PrimitiveArray::new(gather::<T>(data, positions, ctx)?, validity).into_array()
        }
    ))
}

/// Decodes each selected run of rows as one contiguous range of values, or `T::default()` for
/// null rows.
fn decode_runs<T: Number + NativePType>(
    data: &PcoData,
    rows: &RowValues,
    slice_start: usize,
    runs: &[(usize, usize)],
    true_count: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Buffer<T>> {
    let mut decoder = BlockDecoder::<T>::new(data, true)?;
    let mut out = BufferMut::<T>::with_capacity_in(true_count, ctx.allocator().clone());
    let mut run_values = BufferMut::<T>::empty();
    for &(start, end) in runs {
        let (start, end) = (slice_start + start, slice_start + end);
        let values = rows.rank(start)..rows.rank(end);
        match rows {
            RowValues::AllValid => decoder.decode_values(values, &mut out)?,
            RowValues::AllNull => out.push_n(T::default(), end - start),
            RowValues::Ranked { bits, .. } => {
                run_values.clear();
                decoder.decode_values(values, &mut run_values)?;
                let mut next_value = 0;
                for row in start..end {
                    if bits.value(row) {
                        out.push(run_values[next_value]);
                        next_value += 1;
                    } else {
                        out.push(T::default());
                    }
                }
            }
        }
    }
    Ok(out.freeze())
}

/// Reads each selected run of rows as one contiguous range of values, as a primitive array with
/// `validity`.
pub(crate) fn decode_runs_array(
    data: &PcoData,
    rows: &RowValues,
    slice_start: usize,
    runs: &[(usize, usize)],
    true_count: usize,
    validity: Validity,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrayRef> {
    Ok(match_number_enum!(
        number_type_from_ptype(data.ptype()),
        NumberType<T> => {
            let values = decode_runs::<T>(data, rows, slice_start, runs, true_count, ctx)?;
            PrimitiveArray::new(values, validity).into_array()
        }
    ))
}
