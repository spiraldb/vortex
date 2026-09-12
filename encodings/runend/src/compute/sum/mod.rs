// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Primitive sums over run-end encoded arrays.
//!
//! Empty arrays and all-null inputs return before decoding the children. Otherwise, each valid
//! run contributes its value multiplied by the length included in the input.
//! All-valid inputs scan the end and value slices directly. Only partially valid inputs use indices.
//!
//! Whole-array sums visit one range, clipped at the array's slice boundaries. Fixed-size groups
//! share a forward cursor. List-view groups can overlap or arrive out of order, so each group
//! locates its first run independently. The shared reduction in [`runs`] clips intersecting runs.
//!
//! Decimal inputs use the fallback. Floating-point multiplication can round differently from
//! repeated addition, as with constant sums.

mod grouped;
mod runs;
mod whole;

use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::fns::sum_v2::SumV2;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DType;
use vortex_array::scalar::Scalar;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_mask::Mask;

use crate::RunEnd;
use crate::RunEndArrayExt;
use crate::RunEndArraySlotsExt;

/// Whole-array and grouped primitive sum kernels for [`RunEnd`].
#[derive(Debug)]
pub(crate) struct RunEndSumKernel;

struct RunEndInputs {
    ends: PrimitiveArray,
    values: PrimitiveArray,
    validity: Mask,
    offset: usize,
}

impl RunEndInputs {
    /// Skip materializing the children when the array is empty or every run is null.
    fn new(array: ArrayView<'_, RunEnd>, ctx: &mut ExecutionCtx) -> VortexResult<Option<Self>> {
        if array.is_empty() {
            return Ok(None);
        }

        let validity = array
            .values()
            .validity()?
            .execute_mask(array.values().len(), ctx)?;
        if validity.all_false() {
            return Ok(None);
        }

        let ends = array.ends().clone().execute::<PrimitiveArray>(ctx)?;
        let values = array.values().clone().execute::<PrimitiveArray>(ctx)?;

        Ok(Some(Self {
            ends,
            values,
            validity,
            offset: array.offset(),
        }))
    }
}

fn empty_partial(aggregate_fn: &AggregateFnRef, dtype: &DType) -> VortexResult<Scalar> {
    let sum_dtype = aggregate_fn
        .return_dtype(dtype)
        .vortex_expect("The primitive sum kernel accepts only supported dtypes");
    partial_scalar(aggregate_fn, Scalar::zero_value(&sum_dtype), true)
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
