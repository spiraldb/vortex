// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Primitive sums over run-end encoded arrays.
//!
//! The kernels decode the run ends and values once, then sum each value weighted by its run
//! length. Grouped sums intersect runs with each group's range. Decimal inputs use the fallback.
//! Float multiplication can round differently from repeated addition, as with constant sums.

mod primitive;

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::GroupedArray;
use vortex_array::aggregate_fn::fns::sum::Sum;
use vortex_array::aggregate_fn::fns::sum_v2::SumV2;
use vortex_array::aggregate_fn::kernels::DynAggregateKernel;
use vortex_array::aggregate_fn::kernels::DynGroupedAggregateKernel;
use vortex_array::scalar::Scalar;
use vortex_error::VortexResult;

use self::primitive::RunEndSums;
use crate::RunEnd;

/// Whole-array and grouped primitive sum kernels for [`RunEnd`].
#[derive(Debug)]
pub(crate) struct RunEndSumKernel;

impl DynAggregateKernel for RunEndSumKernel {
    fn aggregate(
        &self,
        aggregate_fn: &AggregateFnRef,
        batch: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<Scalar>> {
        let Some(options) = aggregate_fn
            .as_opt::<Sum>()
            .or_else(|| aggregate_fn.as_opt::<SumV2>())
        else {
            return Ok(None);
        };
        let Some(array) = batch.as_opt::<RunEnd>() else {
            return Ok(None);
        };
        if !batch.dtype().is_primitive() {
            return Ok(None);
        }

        let sums = RunEndSums::new(array, ctx, options.skip_nans)?;
        let (sum, is_empty) = sums.sum(0..batch.len());
        Ok(Some(partial_scalar(aggregate_fn, sum, is_empty)?))
    }
}

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

        let ranges = groups.group_ranges(ctx)?;
        let validity = groups.group_validity(ctx)?;
        let sums = RunEndSums::new(elements, ctx, options.skip_nans)?;
        let (results, empty_groups) = sums.grouped_sum(&ranges, &validity);

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

fn partial_scalar(
    aggregate_fn: &AggregateFnRef,
    sum: Scalar,
    is_empty: bool,
) -> VortexResult<Scalar> {
    if aggregate_fn.is::<SumV2>() {
        SumV2::partial_from_sum(sum, is_empty)
    } else {
        Ok(sum)
    }
}

#[cfg(test)]
mod tests;
