// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;

use super::MinMax;
use super::MinMaxPartial;
use super::MinMaxResult;
use crate::Columnar;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::aggregate_fn::Accumulator;
use crate::aggregate_fn::AggregateArgs;
use crate::aggregate_fn::AggregateFnVTable;
use crate::aggregate_fn::DynAccumulator;
use crate::aggregate_fn::ExtensionAggregate;
use crate::aggregate_fn::NumericalAggregateOpts;
use crate::aggregate_fn::session::AggregateFnSessionExt;
use crate::dtype::Nullability;
use crate::dtype::extension::ExtDTypeRef;
use crate::scalar::Scalar;

/// Accumulate the extrema of an extension batch through the session's
/// `ExtensionAggregate<MinMax>` for its type, as extension scalars.
///
/// Extension types without one registered have no extrema: their storage order need not be their
/// own.
pub(super) fn accumulate_extension(
    args: AggregateArgs<'_, NumericalAggregateOpts>,
    partial: &mut MinMaxPartial,
    ext_dtype: &ExtDTypeRef,
    batch: &Columnar,
    ctx: &mut ExecutionCtx,
) -> VortexResult<()> {
    let ext_min_max = ExtensionAggregate::for_ext_id(MinMax, ext_dtype.id());
    if ctx
        .session()
        .aggregate_fns()
        .find_plugin(&ext_min_max.id())
        .is_none()
    {
        return Ok(());
    }

    let mut acc = Accumulator::try_new(ext_min_max, *args.options, batch.dtype().clone())?;
    let array = match batch {
        Columnar::Canonical(canonical) => canonical.clone().into_array(),
        Columnar::Constant(constant) => constant.clone().into_array(),
    };
    acc.accumulate(&array, ctx)?;
    let non_nullable_ext_dtype = ext_dtype.with_nullability(Nullability::NonNullable);
    let local =
        MinMaxResult::from_scalar(&acc.finish()?)?.map(|MinMaxResult { min, max }| MinMaxResult {
            min: Scalar::extension_ref(non_nullable_ext_dtype.clone(), min),
            max: Scalar::extension_ref(non_nullable_ext_dtype, max),
        });
    partial.merge(args, local);
    Ok(())
}
