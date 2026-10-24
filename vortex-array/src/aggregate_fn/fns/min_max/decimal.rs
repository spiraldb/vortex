// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use itertools::Itertools;
use vortex_error::VortexResult;
use vortex_mask::Mask;

use super::MinMaxPartial;
use super::MinMaxResult;
use crate::ExecutionCtx;
use crate::aggregate_fn::AggregateArgs;
use crate::aggregate_fn::NumericalAggregateOpts;
use crate::arrays::DecimalArray;
use crate::dtype::DecimalDType;
use crate::dtype::NativeDecimalType;
use crate::dtype::Nullability::NonNullable;
use crate::match_each_decimal_value_type;
use crate::scalar::DecimalValue;
use crate::scalar::Scalar;

pub(super) fn accumulate_decimal(
    args: AggregateArgs<'_, NumericalAggregateOpts>,
    partial: &mut MinMaxPartial,
    array: &DecimalArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<()> {
    match_each_decimal_value_type!(array.values_type(), |T| {
        let local = compute_min_max_with_validity::<T>(array, ctx)?;
        partial.merge(args, local);
        Ok(())
    })
}

fn compute_min_max_with_validity<D>(
    array: &DecimalArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<MinMaxResult>>
where
    D: Into<DecimalValue> + NativeDecimalType,
{
    Ok(
        match array
            .as_ref()
            .validity()?
            .execute_mask(array.as_ref().len(), ctx)?
        {
            Mask::AllTrue(_) => compute_min_max(array.buffer::<D>().iter(), array.decimal_dtype())?,
            Mask::AllFalse(_) => None,
            Mask::Values(v) => compute_min_max(
                array
                    .buffer::<D>()
                    .iter()
                    .zip(v.bit_buffer().iter())
                    .filter_map(|(v, m)| m.then_some(v)),
                array.decimal_dtype(),
            )?,
        },
    )
}

fn compute_min_max<'a, T>(
    iter: impl Iterator<Item = &'a T>,
    decimal_dtype: DecimalDType,
) -> VortexResult<Option<MinMaxResult>>
where
    T: Into<DecimalValue> + NativeDecimalType + Ord + Copy + 'a,
{
    // The stored values are not guaranteed to fit the precision (for example, when read from a
    // file), so return an error instead of panicking.
    Ok(match iter.minmax_by(|a, b| a.cmp(b)) {
        itertools::MinMaxResult::NoElements => None,
        itertools::MinMaxResult::OneElement(&x) => {
            let scalar = Scalar::try_decimal(x.into(), decimal_dtype, NonNullable)?;
            Some(MinMaxResult {
                min: scalar.clone(),
                max: scalar,
            })
        }
        itertools::MinMaxResult::MinMax(&min, &max) => Some(MinMaxResult {
            min: Scalar::try_decimal(min.into(), decimal_dtype, NonNullable)?,
            max: Scalar::try_decimal(max.into(), decimal_dtype, NonNullable)?,
        }),
    })
}
