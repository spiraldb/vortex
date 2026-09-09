// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Grouped aggregation with traversal selected by the group layout.
//!
//! Fixed-size groups share a forward run cursor. List-view ranges can overlap or arrive out of
//! order, so they locate their runs independently. Both paths weight runs by their intersection
//! with the group, and skip null groups before visiting any runs.

use std::ops::Range;

use itertools::Either;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::GroupRanges;
use vortex_array::aggregate_fn::GroupedArray;
use vortex_array::aggregate_fn::fns::sum::Sum;
use vortex_array::aggregate_fn::fns::sum_v2::SumV2;
use vortex_array::aggregate_fn::kernels::DynGroupedAggregateKernel;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::IntegerPType;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::Nullability::Nullable;
use vortex_array::match_each_native_ptype;
use vortex_array::match_each_unsigned_integer_ptype;
use vortex_array::validity::Validity;
use vortex_buffer::BitBuffer;
use vortex_buffer::BitBufferMut;
use vortex_error::VortexResult;
use vortex_mask::AllOr;
use vortex_mask::Mask;

use super::RunEndInputs;
use super::RunEndSumKernel;
use super::empty_partial;
use super::runs::add_float_run;
use super::runs::add_signed_run;
use super::runs::add_unsigned_run;
use super::runs::sum_next_range;
use super::runs::sum_range;
use crate::RunEnd;

impl DynGroupedAggregateKernel for RunEndSumKernel {
    fn grouped_aggregate(
        &self,
        aggregate_fn: &AggregateFnRef,
        groups: &GroupedArray,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        let Some(options) = aggregate_fn
            .as_opt::<Sum>()
            .or_else(|| aggregate_fn.as_opt::<SumV2>())
        else {
            return Ok(None);
        };
        let Some(elements) = groups.elements().as_opt::<RunEnd>() else {
            return Ok(None);
        };
        if !groups.elements().dtype().is_primitive() {
            return Ok(None);
        }

        let validity = groups.group_validity(ctx)?;
        let runs = if validity.all_false() {
            None
        } else {
            RunEndInputs::new(elements, ctx)?
        };
        let Some(runs) = runs else {
            let partial = empty_partial(aggregate_fn, groups.elements().dtype())?;
            let partials = ConstantArray::new(partial, groups.len()).into_array();
            let validity = Validity::from_mask(validity, Nullable).to_array(groups.len());
            return Ok(Some(partials.mask(validity)?));
        };

        let ranges = groups.group_ranges(ctx)?;
        let valid_runs = runs.validity.indices();

        let (results, empty_groups) = match_each_unsigned_integer_ptype!(runs.ends.ptype(), |E| {
            let ends = runs.ends.as_slice::<E>();
            match_each_native_ptype!(runs.values.ptype(),
                unsigned: |T| {
                    sum_groups(ends, runs.values.as_slice::<T>(), valid_runs, &ranges,
                        &validity, runs.offset, add_unsigned_run)
                },
                signed: |T| {
                    sum_groups(ends, runs.values.as_slice::<T>(), valid_runs, &ranges,
                        &validity, runs.offset, add_signed_run)
                },
                floating: |T| {
                    sum_groups(ends, runs.values.as_slice::<T>(), valid_runs, &ranges,
                        &validity, runs.offset,
                        |sum, value, len| add_float_run(sum, value, len, options.skip_nans))
                }
            )
        });

        let results = results.into_array();
        if aggregate_fn.is::<SumV2>() {
            Ok(Some(SumV2::partials_from_sums(
                results,
                empty_groups,
                validity,
                ctx,
            )?))
        } else {
            Ok(Some(results))
        }
    }
}

fn sum_groups<E: IntegerPType, T: NativePType, A: NativePType>(
    ends: &[E],
    values: &[T],
    validity: AllOr<&[usize]>,
    ranges: &GroupRanges,
    group_validity: &Mask,
    offset: usize,
    add_run: impl Fn(A, T, usize) -> Option<A>,
) -> (PrimitiveArray, BitBuffer) {
    match ranges {
        // Consecutive groups reuse the cursor, including a run split across group boundaries.
        GroupRanges::FixedSizeList { .. } => {
            let indices = match validity {
                AllOr::All => Either::Left(0..ends.len()),
                AllOr::None => Either::Left(0..0),
                AllOr::Some(indices) => Either::Right(indices.iter().copied()),
            };
            let mut indices = indices.peekable();

            collect_group_sums(ranges, group_validity, offset, |range| {
                sum_next_range(ends, values, &mut indices, range, &add_run)
            })
        }
        // These ranges can overlap or go backwards, so each group seeks independently.
        GroupRanges::ListView { .. } => {
            collect_group_sums(ranges, group_validity, offset, |range| {
                sum_range(ends, values, &validity, range, &add_run)
            })
        }
    }
}

fn collect_group_sums<A: NativePType>(
    ranges: &GroupRanges,
    group_validity: &Mask,
    offset: usize,
    mut sum_group: impl FnMut(Range<usize>) -> (Option<A>, bool),
) -> (PrimitiveArray, BitBuffer) {
    let mut empty_groups = BitBufferMut::new_unset(ranges.len());
    let sums =
        PrimitiveArray::from_option_iter(ranges.iter().zip(group_validity.iter()).enumerate().map(
            |(index, ((start, len), valid))| {
                if !valid {
                    return None;
                }

                let (sum, is_empty) = sum_group(offset + start..offset + start + len);
                empty_groups.set_to(index, is_empty);
                sum
            },
        ));

    (sums, empty_groups.freeze())
}
