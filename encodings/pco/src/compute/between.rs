// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use pco::data_types::NumberType;
use pco::match_number_enum;
use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::half;
use vortex_array::scalar_fn::fns::between::BetweenKernel;
use vortex_array::scalar_fn::fns::between::BetweenOptions;
use vortex_array::scalar_fn::fns::between::StrictComparison;
use vortex_array::scalar_fn::fns::operators::CompareOperator;
use vortex_buffer::BitBufferMut;
use vortex_error::VortexResult;

use crate::Pco;
use crate::array::number_type_from_ptype;
use crate::compute::compare::decide;
use crate::compute::compare::evaluate_by_block;

/// Answers whole blocks from their stored minimum and maximum, decoding only blocks that
/// straddle a bound. Needs the v2 block stats.
impl BetweenKernel for Pco {
    fn between(
        array: ArrayView<'_, Self>,
        lower: &ArrayRef,
        upper: &ArrayRef,
        options: &BetweenOptions,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        if !array
            .index
            .as_ref()
            .is_some_and(|index| index.block_stats.is_some())
        {
            return Ok(None);
        }
        let (Some(lower_constant), Some(upper_constant)) =
            (lower.as_constant(), upper.as_constant())
        else {
            return Ok(None);
        };
        let (Some(lower_constant), Some(upper_constant)) = (
            lower_constant.as_primitive_opt(),
            upper_constant.as_primitive_opt(),
        ) else {
            return Ok(None);
        };
        if lower_constant.ptype() != array.ptype() || upper_constant.ptype() != array.ptype() {
            return Ok(None);
        }
        let nullable = array.dtype().is_nullable()
            || lower.dtype().is_nullable()
            || upper.dtype().is_nullable();
        let lower_op = match options.lower_strict {
            StrictComparison::Strict => CompareOperator::Gt,
            StrictComparison::NonStrict => CompareOperator::Gte,
        };
        let upper_op = match options.upper_strict {
            StrictComparison::Strict => CompareOperator::Lt,
            StrictComparison::NonStrict => CompareOperator::Lte,
        };

        match_number_enum!(
            number_type_from_ptype(array.ptype()),
            NumberType<T> => {
                let (Some(lo), Some(hi)) =
                    (lower_constant.typed_value::<T>(), upper_constant.typed_value::<T>())
                else {
                    return Ok(None);
                };
                evaluate_by_block::<T>(
                    array,
                    |min, max| {
                        match (
                            decide(min, max, lo, lower_op),
                            decide(min, max, hi, upper_op),
                        ) {
                            (Some(false), _) | (_, Some(false)) => Some(false),
                            (Some(true), Some(true)) => Some(true),
                            _ => None,
                        }
                    },
                    |values| between_values(values, lo, hi, options),
                    nullable,
                    ctx,
                )
            }
        )
    }
}

fn between_values<T: NativePType>(
    values: &[T],
    lo: T,
    hi: T,
    options: &BetweenOptions,
) -> BitBufferMut {
    // Dispatch outside the loop so each closure vectorizes a single predicate.
    let n = values.len();
    match (options.lower_strict, options.upper_strict) {
        (StrictComparison::Strict, StrictComparison::Strict) => {
            BitBufferMut::collect_bool(n, |i| values[i].is_gt(lo) && values[i].is_lt(hi))
        }
        (StrictComparison::Strict, StrictComparison::NonStrict) => {
            BitBufferMut::collect_bool(n, |i| values[i].is_gt(lo) && values[i].is_le(hi))
        }
        (StrictComparison::NonStrict, StrictComparison::Strict) => {
            BitBufferMut::collect_bool(n, |i| values[i].is_ge(lo) && values[i].is_lt(hi))
        }
        (StrictComparison::NonStrict, StrictComparison::NonStrict) => {
            BitBufferMut::collect_bool(n, |i| values[i].is_ge(lo) && values[i].is_le(hi))
        }
    }
}
