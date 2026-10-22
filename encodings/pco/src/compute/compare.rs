// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use pco::data_types::Number;
use pco::data_types::NumberType;
use pco::match_number_enum;
use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::BoolArray;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::half;
use vortex_array::scalar_fn::fns::binary::CompareKernel;
use vortex_array::scalar_fn::fns::operators::CompareOperator;
use vortex_array::validity::Validity;
use vortex_buffer::BitBufferMut;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::Pco;
use crate::PcoArrayExt;
use crate::array::BlockDecoder;
use crate::array::number_type_from_ptype;

/// Answers whole blocks from their stored minimum and maximum, decoding only blocks that
/// straddle the constant. Needs the v2 block stats.
impl CompareKernel for Pco {
    fn compare(
        lhs: ArrayView<'_, Self>,
        rhs: &ArrayRef,
        operator: CompareOperator,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        if !lhs
            .index
            .as_ref()
            .is_some_and(|index| index.block_stats.is_some())
        {
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
                evaluate_by_block::<T>(
                    lhs,
                    |min, max| decide(min, max, value, operator),
                    |values| compare_values(values, value, operator),
                    nullable,
                    ctx,
                )
            }
        )
    }
}

/// Decides a whole block from its `[min, max]` under Vortex's total order, or `None` if the
/// block straddles `value`.
pub(super) fn decide<T: NativePType>(
    min: T,
    max: T,
    value: T,
    op: CompareOperator,
) -> Option<bool> {
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

/// Evaluates a predicate block by block: blocks that `decide_block` settles from their
/// `[min, max]` are filled without decoding, and only runs of undecided blocks are decoded and
/// passed to `evaluate`.
///
/// Declines when most blocks are undecided, since decoding everything and running the vectorized
/// primitive kernel is then faster.
pub(super) fn evaluate_by_block<T: Number + NativePType>(
    array: ArrayView<'_, Pco>,
    decide_block: impl Fn(T, T) -> Option<bool>,
    evaluate: impl Fn(&[T]) -> BitBufferMut,
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
        .ok_or_else(|| vortex_err!("Pco block predicates require block stats"))?;
    let block_decision = |block: usize| decide_block(mins[block], maxes[block]);
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
    let mut decoder = BlockDecoder::<T>::new(data, true)?;
    let mut decoded = BufferMut::<T>::empty();
    for (page, entry) in data
        .entries
        .iter()
        .enumerate()
        .skip(data.page_of(values.start))
    {
        if entry.values.start >= values.end {
            break;
        }
        let local = values.start.max(entry.values.start) - entry.values.start
            ..values.end.min(entry.values.end) - entry.values.start;
        let block_len = entry.block_len;
        let mut block = local.start / block_len;
        while block * block_len < local.end {
            if let Some(result) = block_decision(entry.first_block + block) {
                let segment =
                    (block * block_len).max(local.start)..((block + 1) * block_len).min(local.end);
                value_bits.append_n(result, segment.len());
                block += 1;
                continue;
            }
            let mut run_end = block + 1;
            while run_end * block_len < local.end
                && block_decision(entry.first_block + run_end).is_none()
            {
                run_end += 1;
            }
            let segment =
                (block * block_len).max(local.start)..(run_end * block_len).min(local.end);
            let decoded_start = block * block_len;
            decoded.clear();
            decoder.append_page_blocks(page, block..run_end, &mut decoded)?;
            let bits =
                evaluate(&decoded[segment.start - decoded_start..segment.end - decoded_start]);
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
