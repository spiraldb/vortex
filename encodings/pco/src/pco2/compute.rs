// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Pco2 kernels that decode only the checkpoint blocks an operation touches.

use std::ops::Range;

use pco::data_types::Number;
use pco::data_types::NumberType;
use pco::match_number_enum;
use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::dict::TakeExecute;
use vortex_array::arrays::filter::FilterKernel;
use vortex_array::arrays::slice::SliceReduce;
use vortex_array::arrays::slice::SliceReduceAdaptor;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::half;
use vortex_array::match_each_integer_ptype;
use vortex_array::optimizer::rules::ParentRuleSet;
use vortex_array::scalar_fn::fns::binary::CompareKernel;
use vortex_array::scalar_fn::fns::operators::CompareOperator;
use vortex_array::validity::Validity;
use vortex_buffer::BitBuffer;
use vortex_buffer::BitBufferMut;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use super::BlockDecoder;
use super::Pco2;
use super::Pco2ArrayExt;
use super::Pco2Data;
use crate::array::number_type_from_ptype;

pub(crate) static RULES: ParentRuleSet<Pco2> =
    ParentRuleSet::new(&[ParentRuleSet::lift(&SliceReduceAdaptor(Pco2))]);

impl SliceReduce for Pco2 {
    fn slice(array: ArrayView<'_, Self>, range: Range<usize>) -> VortexResult<Option<ArrayRef>> {
        Ok(Some(
            Pco2::try_new(
                array.dtype().clone(),
                array._slice(range.start, range.end),
                array.unsliced_validity(),
            )?
            .into_array(),
        ))
    }
}

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

fn row_values(array: ArrayView<'_, Pco2>, ctx: &mut ExecutionCtx) -> VortexResult<RowValues> {
    Ok(RowValues::new(
        array
            .unsliced_validity()
            .execute_mask(array.unsliced_n_rows(), ctx)?,
    ))
}

/// Marks an output position that reads no value.
const NO_VALUE: usize = usize::MAX;

/// Reads the valid values at `positions`, or `T::default()` for [`NO_VALUE`], decoding every
/// touched checkpoint block once. Runs of adjacent blocks decode together.
fn gather<T: Number + NativePType>(
    data: &Pco2Data,
    positions: &[usize],
    ctx: &mut ExecutionCtx,
) -> VortexResult<Buffer<T>> {
    let interval = data.interval();
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

    let mut decoder = BlockDecoder::<T>::new(data)?;
    let mut decoded = BufferMut::<T>::with_capacity(touched.len() * interval);
    // For each touched block, where its values start in `decoded` and among the valid values.
    let mut decoded_starts = Vec::with_capacity(touched.len());
    let mut value_starts = Vec::with_capacity(touched.len());
    let mut run_start = 0;
    while run_start < touched.len() {
        let page = data.index.partition_point(|entry| {
            entry.first_block + data.blocks_in_page(entry) <= touched[run_start]
        });
        let entry = &data.index[page];
        let page_blocks = entry.first_block..entry.first_block + data.blocks_in_page(entry);
        let mut run_end = run_start + 1;
        while run_end < touched.len()
            && touched[run_end] == touched[run_end - 1] + 1
            && page_blocks.contains(&touched[run_end])
        {
            run_end += 1;
        }

        let blocks =
            touched[run_start] - entry.first_block..touched[run_end - 1] + 1 - entry.first_block;
        for (offset, block) in blocks.clone().enumerate() {
            decoded_starts.push(decoded.len() + offset * interval);
            value_starts.push(entry.values.start + block * interval);
        }
        let n = (blocks.end * interval).min(entry.values.len()) - blocks.start * interval;
        let old_len = decoded.len();
        decoded.reserve(n);
        // SAFETY: pco either writes every element of the new tail or returns an error, in which
        // case `decoded` is dropped unread.
        unsafe { decoded.set_len(old_len + n) };
        decoder.decode_page_blocks(page, blocks, &mut decoded[old_len..])?;
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
                .map_err(|_| vortex_err!("Pco2 block {block} was not decoded"))?;
        }
        out.push(decoded[decoded_starts[cursor] + position - value_starts[cursor]]);
    }
    Ok(out.freeze())
}

fn gather_array(
    data: &Pco2Data,
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
    data: &Pco2Data,
    rows: &RowValues,
    slice_start: usize,
    runs: &[(usize, usize)],
    true_count: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Buffer<T>> {
    let mut decoder = BlockDecoder::<T>::new(data)?;
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

/// Filters by decoding each selected run of rows as one contiguous range of values.
fn filter_runs(
    data: &Pco2Data,
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

impl TakeExecute for Pco2 {
    fn take(
        array: ArrayView<'_, Self>,
        indices: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        let primitive_indices = indices.clone().execute::<PrimitiveArray>(ctx)?;
        let indices_mask = primitive_indices
            .validity()?
            .execute_mask(primitive_indices.len(), ctx)?;
        let rows = row_values(array, ctx)?;
        let len = array.len();
        let slice_start = array.slice_start();

        let mut positions = Vec::with_capacity(primitive_indices.len());
        match_each_integer_ptype!(primitive_indices.ptype(), |I| {
            for (i, &index) in primitive_indices.as_slice::<I>().iter().enumerate() {
                if !indices_mask.value(i) {
                    positions.push(NO_VALUE);
                    continue;
                }
                // Widening to i128 first accepts every index type, signed or not.
                let row = usize::try_from(i128::from(index))
                    .ok()
                    .filter(|&row| row < len)
                    .ok_or_else(|| {
                        vortex_err!("Pco2 take index {index} out of bounds for {len}")
                    })?;
                positions.push(rows.value_index(slice_start + row).unwrap_or(NO_VALUE));
            }
        });

        let validity = array.validity()?.take(indices)?;
        gather_array(array.data(), &positions, validity, ctx).map(Some)
    }
}

impl FilterKernel for Pco2 {
    fn filter(
        array: ArrayView<'_, Self>,
        mask: &Mask,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        let Some(mask_values) = mask.values() else {
            return Ok(None);
        };
        let rows = row_values(array, ctx)?;
        let slice_start = array.slice_start();
        let interval = array.interval();

        // When the selection touches most blocks, decoding everything and filtering the
        // primitive array beats gathering value by value.
        let all_values = rows.rank(slice_start)..rows.rank(array.slice_stop());
        let n_blocks = all_values.len().div_ceil(interval);
        let mut touched_blocks = 0;
        let mut last_block = None;
        for &(start, end) in mask_values.slices() {
            let values = rows.rank(slice_start + start)..rows.rank(slice_start + end);
            if values.is_empty() {
                continue;
            }
            let first = values.start / interval;
            let last = (values.end - 1) / interval;
            touched_blocks += last - first + 1 - usize::from(last_block == Some(first));
            last_block = Some(last);
        }
        if 2 * touched_blocks > n_blocks {
            return Ok(None);
        }
        let validity = array.validity()?.filter(mask)?;

        // Runs that rarely share a block decode fastest as contiguous ranges.
        if mask_values.slices().len() <= touched_blocks {
            return filter_runs(
                array.data(),
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
        gather_array(array.data(), &positions, validity, ctx).map(Some)
    }
}

impl CompareKernel for Pco2 {
    fn compare(
        lhs: ArrayView<'_, Self>,
        rhs: &ArrayRef,
        operator: CompareOperator,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        if !lhs.metadata.block_stats {
            return Ok(None);
        }
        let Some(constant) = rhs.as_constant() else {
            return Ok(None);
        };
        let Some(constant) = constant.as_primitive_opt() else {
            return Ok(None);
        };
        if constant.ptype() != lhs.ptype() {
            return Ok(None);
        }
        let nullable = lhs.dtype().is_nullable() || rhs.dtype().is_nullable();

        match_number_enum!(
            number_type_from_ptype(lhs.ptype()),
            NumberType<T> => {
                let Some(value) = constant.typed_value::<T>() else {
                    return Ok(None);
                };
                compare_constant::<T>(lhs, value, operator, nullable, ctx)
            }
        )
    }
}

/// Decides a whole block from its `[min, max]` under Vortex's total order, or `None` if the
/// block straddles `value`.
fn decide<T: NativePType>(min: T, max: T, value: T, op: CompareOperator) -> Option<bool> {
    match op {
        CompareOperator::Eq => {
            if value.is_lt(min) || value.is_gt(max) {
                Some(false)
            } else if min.is_eq(value) && max.is_eq(value) {
                Some(true)
            } else {
                None
            }
        }
        CompareOperator::NotEq => decide(min, max, value, CompareOperator::Eq).map(|eq| !eq),
        CompareOperator::Lt => {
            if max.is_lt(value) {
                Some(true)
            } else if min.is_ge(value) {
                Some(false)
            } else {
                None
            }
        }
        CompareOperator::Lte => {
            if max.is_le(value) {
                Some(true)
            } else if min.is_gt(value) {
                Some(false)
            } else {
                None
            }
        }
        CompareOperator::Gt => {
            if min.is_gt(value) {
                Some(true)
            } else if max.is_le(value) {
                Some(false)
            } else {
                None
            }
        }
        CompareOperator::Gte => {
            if min.is_ge(value) {
                Some(true)
            } else if max.is_lt(value) {
                Some(false)
            } else {
                None
            }
        }
    }
}

fn compare_values<T: NativePType>(values: &[T], value: T, op: CompareOperator) -> BitBufferMut {
    // Dispatch outside the loop so each closure vectorizes a single predicate.
    match op {
        CompareOperator::Eq => BitBufferMut::collect_bool(values.len(), |i| values[i].is_eq(value)),
        CompareOperator::NotEq => {
            BitBufferMut::collect_bool(values.len(), |i| !values[i].is_eq(value))
        }
        CompareOperator::Lt => BitBufferMut::collect_bool(values.len(), |i| values[i].is_lt(value)),
        CompareOperator::Lte => {
            BitBufferMut::collect_bool(values.len(), |i| values[i].is_le(value))
        }
        CompareOperator::Gt => BitBufferMut::collect_bool(values.len(), |i| values[i].is_gt(value)),
        CompareOperator::Gte => {
            BitBufferMut::collect_bool(values.len(), |i| values[i].is_ge(value))
        }
    }
}

/// Compares against a constant block by block: blocks whose `[min, max]` decide the predicate
/// are filled without decoding, and only runs of straddling blocks are decoded.
///
/// Declines when most blocks straddle the constant, since decoding everything and running the
/// vectorized primitive comparison is then faster.
fn compare_constant<T: Number + NativePType>(
    array: ArrayView<'_, Pco2>,
    value: T,
    op: CompareOperator,
    nullable: bool,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ArrayRef>> {
    let data = array.data();
    let unsliced_mask = array
        .unsliced_validity()
        .execute_mask(array.unsliced_n_rows(), ctx)?;
    let values = data.value_range(&unsliced_mask);
    let (mins, maxes) = data
        .block_stats::<T>()?
        .ok_or_else(|| vortex_err!("Pco2 compare requires block stats"))?;
    let interval = data.interval();
    let block_decision = |block: usize| decide(mins[block], maxes[block], value, op);
    if !values.is_empty() {
        let first_page = data.page_of(values.start);
        let last_page = data.page_of(values.end - 1);
        let blocks =
            data.block_of(first_page, values.start)..data.block_of(last_page, values.end - 1) + 1;
        let undecided = blocks
            .clone()
            .filter(|&block| block_decision(block).is_none())
            .count();
        if 2 * undecided > blocks.len() {
            return Ok(None);
        }
    }

    let mut value_bits = BitBufferMut::with_capacity(values.len());
    let mut decoder = BlockDecoder::<T>::new(data)?;
    for (page, entry) in data
        .index
        .iter()
        .enumerate()
        .skip(data.page_of(values.start))
    {
        if entry.values.start >= values.end {
            break;
        }
        let local = values.start.max(entry.values.start) - entry.values.start
            ..values.end.min(entry.values.end) - entry.values.start;
        let mut block = local.start / interval;
        while block * interval < local.end {
            if let Some(result) = block_decision(entry.first_block + block) {
                let segment =
                    (block * interval).max(local.start)..((block + 1) * interval).min(local.end);
                value_bits.append_n(result, segment.len());
                block += 1;
                continue;
            }
            let mut run_end = block + 1;
            while run_end * interval < local.end
                && block_decision(entry.first_block + run_end).is_none()
            {
                run_end += 1;
            }
            let segment = (block * interval).max(local.start)..(run_end * interval).min(local.end);
            let decoded_start = block * interval;
            let decoded = decoder.decode_page_blocks_scratch(page, block..run_end)?;
            let bits = compare_values(
                &decoded[segment.start - decoded_start..segment.end - decoded_start],
                value,
                op,
            );
            value_bits.append_buffer(&bits.freeze());
            block = run_end;
        }
    }
    let value_bits = value_bits.freeze();

    let len = array.len();
    let bits = match unsliced_mask.slice(array.slice_start()..array.slice_stop()) {
        Mask::AllTrue(_) => value_bits,
        Mask::AllFalse(_) => BitBufferMut::new_unset(len).freeze(),
        Mask::Values(row_validity) => {
            let mut next_value = 0;
            BitBufferMut::collect_bool(len, |row| {
                if row_validity.value(row) {
                    next_value += 1;
                    value_bits.value(next_value - 1)
                } else {
                    false
                }
            })
            .freeze()
        }
    };
    let validity = match (nullable, array.validity()?) {
        (false, _) => Validity::NonNullable,
        (true, Validity::NonNullable) => Validity::AllValid,
        (true, validity) => validity,
    };
    Ok(Some(BoolArray::new(bits, validity).into_array()))
}
