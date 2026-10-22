// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;

use pco::FULL_BATCH_N;
use pco::data_types::Number;
use pco::data_types::NumberType;
use pco::match_number_enum;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::fns::is_constant::IsConstant;
use vortex_array::aggregate_fn::kernels::DynAggregateKernel;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::half;
use vortex_array::scalar::Scalar;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;

use crate::Pco;
use crate::PcoArrayExt;
use crate::PcoData;
use crate::array::BlockDecoder;
use crate::array::number_type_from_ptype;

/// Decides `is_constant` without decoding whole arrays. With the v2 block stats, blocks whose
/// minimum and maximum differ, or differ from another block's, settle the answer, and only the
/// blocks a slice cuts through are decoded. Without them, blocks are decoded in order until one
/// holds a different value.
#[derive(Debug)]
pub(crate) struct PcoIsConstantKernel;

impl DynAggregateKernel for PcoIsConstantKernel {
    fn aggregate(
        &self,
        aggregate_fn: &AggregateFnRef,
        batch: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<Scalar>> {
        if !aggregate_fn.is::<IsConstant>() {
            return Ok(None);
        }
        let Some(array) = batch.as_opt::<Pco>() else {
            return Ok(None);
        };
        let mask = array
            .unsliced_validity()
            .execute_mask(array.unsliced_n_rows(), ctx)?;
        let data = array.data();
        let values = data.value_range(&mask);
        // With nulls, the array is constant only if every row is null.
        let is_constant = if values.len() < array.len() {
            values.is_empty()
        } else {
            match_number_enum!(
                number_type_from_ptype(data.ptype()),
                NumberType<T> => { values_are_constant::<T>(data, values)? }
            )
        };
        Ok(Some(IsConstant::make_partial(batch, is_constant, ctx)?))
    }
}

/// Whether valid values `values` of `data` are all bitwise equal.
fn values_are_constant<T: Number + NativePType>(
    data: &PcoData,
    values: Range<usize>,
) -> VortexResult<bool> {
    if values.len() <= 1 {
        return Ok(true);
    }
    let mut decoder = BlockDecoder::<T>::new(data, values.len() < data.n_values())?;
    let mut decoded = BufferMut::<T>::empty();
    let mut value = None;
    let first_block = data.block_of(data.page_of(values.start), values.start);
    let last_block = data.block_of(data.page_of(values.end - 1), values.end - 1);
    let Some((mins, maxes)) = data.block_stats::<T>()? else {
        // Most arrays that are not constant differ within their first batch, so check it before
        // decoding a whole block.
        let page = data.page_of(values.start);
        let entry = &data.entries[page];
        let start = values.start - entry.values.start;
        let end = (start + FULL_BATCH_N).min(values.end.min(entry.values.end) - entry.values.start);
        decoder.append_page_values(page, start..end, &mut decoded)?;
        let first = decoded[0];
        if !all_equal_to(&decoded, first) {
            return Ok(false);
        }
        value = Some(first);
        for block in first_block..=last_block {
            if !block_is_constant(&mut decoder, block, &values, &mut decoded, &mut value)? {
                return Ok(false);
            }
        }
        return Ok(true);
    };

    // Blocks the slice cuts through may hold differing values outside of it, so only their
    // decoded values can rule them out.
    let mut cut_blocks = Vec::new();
    for block in first_block..=last_block {
        let (min, max) = (mins[block], maxes[block]);
        if min.is_eq(max) {
            if !value.get_or_insert(min).is_eq(min) {
                return Ok(false);
            }
            continue;
        }
        let range = block_values(data, block);
        if range.start >= values.start && range.end <= values.end {
            return Ok(false);
        }
        cut_blocks.push(block);
    }
    for block in cut_blocks {
        if !block_is_constant(&mut decoder, block, &values, &mut decoded, &mut value)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether the values of `block` within `values` all equal `value`, which is set to the first of
/// them if unset.
fn block_is_constant<T: Number + NativePType>(
    decoder: &mut BlockDecoder<'_, T>,
    block: usize,
    values: &Range<usize>,
    decoded: &mut BufferMut<T>,
    value: &mut Option<T>,
) -> VortexResult<bool> {
    let data = decoder.data();
    let page = data.page_of_block(block);
    let page_start = data.entries[page].values.start;
    let range = block_values(data, block);
    let local = range.start.max(values.start) - page_start..range.end.min(values.end) - page_start;
    decoded.clear();
    decoder.append_page_values(page, local, decoded)?;
    let first = *value.get_or_insert(decoded[0]);
    Ok(all_equal_to(decoded, first))
}

/// The valid values held by array-wide block `block`.
fn block_values(data: &PcoData, block: usize) -> Range<usize> {
    let entry = &data.entries[data.page_of_block(block)];
    let local = block - entry.first_block;
    let range = entry.block_values(local..local + 1);
    entry.values.start + range.start..entry.values.start + range.end
}

fn all_equal_to<T: NativePType>(values: &[T], value: T) -> bool {
    // Fold fixed-size chunks without short-circuiting so the comparisons vectorize.
    values
        .chunks(64)
        .all(|chunk| chunk.iter().fold(true, |equal, v| equal & v.is_eq(value)))
}
