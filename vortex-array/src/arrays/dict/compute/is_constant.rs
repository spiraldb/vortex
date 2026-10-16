// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;
use vortex_mask::Mask;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::aggregate_fn::AggregateFnRef;
use crate::aggregate_fn::fns::is_constant::IsConstant;
use crate::aggregate_fn::fns::is_constant::is_constant;
use crate::aggregate_fn::kernels::DynAggregateKernel;
use crate::arrays::Dict;
use crate::arrays::dict::DictArrayExt;
use crate::arrays::dict::DictArraySlotsExt;
use crate::scalar::Scalar;

/// Dict-specific is_constant kernel.
///
/// If codes are constant, the whole array is constant.
/// When all dictionary values are referenced, is_constant can be computed directly on the values
/// array. Otherwise, unreferenced values are filtered out first.
#[derive(Debug)]
pub(crate) struct DictIsConstantKernel;

impl DynAggregateKernel for DictIsConstantKernel {
    fn aggregate(
        &self,
        aggregate_fn: &AggregateFnRef,
        batch: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<Scalar>> {
        if !aggregate_fn.is::<IsConstant>() {
            return Ok(None);
        }

        let Some(dict) = batch.as_opt::<Dict>() else {
            return Ok(None);
        };

        // If codes are constant, only one dictionary value is referenced → constant.
        if is_constant(dict.codes(), ctx)? {
            return Ok(Some(IsConstant::make_partial(batch, true, ctx)?));
        }

        // Otherwise, check the values array. Filter to only referenced values if needed.
        let values = if dict.has_all_values_referenced() {
            dict.values().clone()
        } else {
            let referenced_mask = dict.compute_referenced_values_mask(true, ctx)?;
            dict.values().filter(Mask::from(referenced_mask))?
        };
        let result = if dict.codes().all_valid(ctx)? {
            is_constant(&values, ctx)?
        } else {
            // Null codes are null rows, so the array is constant only if every referenced value
            // is null as well.
            values.all_invalid(ctx)?
        };

        Ok(Some(IsConstant::make_partial(batch, result, ctx)?))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;

    use crate::ArrayRef;
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::aggregate_fn::Accumulator;
    use crate::aggregate_fn::DynAccumulator;
    use crate::aggregate_fn::EmptyOptions;
    use crate::aggregate_fn::fns::is_constant::IsConstant;
    use crate::aggregate_fn::fns::is_constant::is_constant;
    use crate::array_session;
    use crate::arrays::ConstantArray;
    use crate::arrays::DictArray;
    use crate::arrays::PrimitiveArray;
    use crate::dtype::Nullability;
    use crate::scalar::Scalar;

    /// Null codes are null rows: the array is constant only if the referenced values are null too.
    #[rstest]
    #[case::null_and_valid_rows(
        ConstantArray::new(Scalar::binary(buffer![], Nullability::NonNullable), 1).into_array(),
        false
    )]
    #[case::all_rows_null(PrimitiveArray::from_option_iter::<u8, _>([None, None]).into_array(), true)]
    fn test_dict_is_constant_with_null_codes(
        #[case] values: ArrayRef,
        #[case] expected: bool,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let max_code = u64::try_from(values.len() - 1)?;
        let codes = PrimitiveArray::from_option_iter([Some(0u64), None, Some(max_code)]);
        let dict = DictArray::try_new(codes.into_array(), values)?.into_array();

        // `is_constant` answers mixed validity up front, so accumulate to reach the kernel.
        let mut acc = Accumulator::try_new(IsConstant, EmptyOptions, dict.dtype().clone())?;
        acc.accumulate(&dict, &mut ctx)?;
        assert_eq!(acc.finish()?, Scalar::from(expected));
        assert_eq!(is_constant(&dict, &mut ctx)?, expected);
        Ok(())
    }
}
