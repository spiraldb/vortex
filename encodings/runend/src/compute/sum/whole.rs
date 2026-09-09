// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Whole-array aggregation over a single logical range.
//!
//! The first and last runs can be clipped by a slice. No group traversal or shared cursor is needed.

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::fns::sum::Sum;
use vortex_array::aggregate_fn::fns::sum_v2::SumV2;
use vortex_array::aggregate_fn::kernels::DynAggregateKernel;
use vortex_array::dtype::DType;
use vortex_array::dtype::IntegerPType;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::Nullability::Nullable;
use vortex_array::match_each_native_ptype;
use vortex_array::match_each_unsigned_integer_ptype;
use vortex_array::scalar::PValue;
use vortex_array::scalar::Scalar;
use vortex_error::VortexResult;
use vortex_mask::AllOr;

use super::RunEndInputs;
use super::RunEndSumKernel;
use super::empty_partial;
use super::partial_scalar;
use super::runs::add_float_run;
use super::runs::add_signed_run;
use super::runs::add_unsigned_run;
use super::runs::sum_range;
use crate::RunEnd;

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

        let Some(runs) = RunEndInputs::new(array, ctx)? else {
            return Ok(Some(empty_partial(aggregate_fn, batch.dtype())?));
        };
        let range = runs.offset..runs.offset + batch.len();
        let valid_runs = runs.validity.indices();

        let (sum, is_empty) = match_each_unsigned_integer_ptype!(runs.ends.ptype(), |E| {
            let ends = runs.ends.as_slice::<E>();
            match_each_native_ptype!(runs.values.ptype(),
                unsigned: |T| {
                    sum_scalar(ends, runs.values.as_slice::<T>(), &valid_runs, range, add_unsigned_run)
                },
                signed: |T| {
                    sum_scalar(ends, runs.values.as_slice::<T>(), &valid_runs, range, add_signed_run)
                },
                floating: |T| {
                    sum_scalar(ends, runs.values.as_slice::<T>(), &valid_runs, range,
                        |sum, value, len| add_float_run(sum, value, len, options.skip_nans))
                }
            )
        });

        Ok(Some(partial_scalar(aggregate_fn, sum, is_empty)?))
    }
}

fn sum_scalar<E: IntegerPType, T: NativePType, A: NativePType + Into<PValue>>(
    ends: &[E],
    values: &[T],
    validity: &AllOr<&[usize]>,
    range: Range<usize>,
    add_run: impl Fn(A, T, usize) -> Option<A>,
) -> (Scalar, bool) {
    let (sum, is_empty) = sum_range(ends, values, validity, range, add_run);
    let sum = match sum {
        Some(sum) => Scalar::primitive(sum, Nullable),
        None => Scalar::null(DType::Primitive(A::PTYPE, Nullable)),
    };

    (sum, is_empty)
}
