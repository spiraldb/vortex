// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Grouped primitive sums over constant elements.
//!
//! Reuse the whole-array accumulator for each group size. Fixed-size groups share one
//! constant partial, and list-view groups avoid slicing the elements before accumulation.

use vortex_error::VortexExpect;
use vortex_error::VortexResult;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::aggregate_fn::AggregateFnRef;
use crate::aggregate_fn::GroupRanges;
use crate::aggregate_fn::GroupedArray;
use crate::aggregate_fn::fns::sum::Sum;
use crate::aggregate_fn::fns::sum_v2::SumV2;
use crate::aggregate_fn::kernels::DynGroupedAggregateKernel;
use crate::arrays::BoolArray;
use crate::arrays::Constant;
use crate::arrays::ConstantArray;
use crate::builders::builder_with_capacity_in;
use crate::builtins::ArrayBuiltins;
use crate::validity::Validity;

/// Grouped primitive sum kernel for constant elements.
#[derive(Debug)]
pub(crate) struct ConstantGroupedSumKernel;

impl DynGroupedAggregateKernel for ConstantGroupedSumKernel {
    fn grouped_aggregate(
        &self,
        aggregate_fn: &AggregateFnRef,
        groups: &GroupedArray,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        if !aggregate_fn.is::<Sum>() && !aggregate_fn.is::<SumV2>() {
            return Ok(None);
        }
        let Some(elements) = groups.elements().as_opt::<Constant>() else {
            return Ok(None);
        };
        if !elements.dtype().is_primitive() {
            return Ok(None);
        }

        let ranges = groups.group_ranges(ctx)?;
        let validity = groups.group_validity(ctx)?;
        let scalar = elements.scalar();
        let mut accumulator = aggregate_fn.accumulator(elements.dtype())?;

        if let GroupRanges::FixedSizeList { size, .. } = ranges {
            let group = ConstantArray::new(scalar.clone(), size).into_array();
            accumulator.accumulate(&group, ctx)?;
            let partials = ConstantArray::new(accumulator.flush()?, groups.len()).into_array();
            let mask = BoolArray::new(validity.to_bit_buffer(), Validity::NonNullable).into_array();
            return Ok(Some(partials.mask(mask)?));
        }

        let partial_dtype = aggregate_fn
            .state_dtype(elements.dtype())
            .vortex_expect("The primitive sum accumulator has a partial dtype");
        let mut partials = builder_with_capacity_in(&partial_dtype, groups.len(), ctx.allocator());
        for ((_, size), valid) in ranges.iter().zip(validity.iter()) {
            if !valid {
                partials.append_null();
                continue;
            }

            let group = ConstantArray::new(scalar.clone(), size).into_array();
            accumulator.accumulate(&group, ctx)?;
            partials.append_scalar(&accumulator.flush()?)?;
        }

        Ok(Some(partials.finish()))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;

    use super::ConstantGroupedSumKernel;
    use crate::ArrayRef;
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::aggregate_fn::AggregateFnVTableExt;
    use crate::aggregate_fn::GroupedArray;
    use crate::aggregate_fn::NumericalAggregateOpts;
    use crate::aggregate_fn::fns::sum::Sum;
    use crate::aggregate_fn::fns::sum_v2::SumV2;
    use crate::aggregate_fn::kernels::DynGroupedAggregateKernel;
    use crate::array_session;
    use crate::arrays::ConstantArray;
    use crate::arrays::FixedSizeListArray;
    use crate::arrays::ListViewArray;
    use crate::arrays::PrimitiveArray;
    use crate::assert_arrays_eq;
    use crate::dtype::DType;
    use crate::dtype::DecimalDType;
    use crate::dtype::Nullability::NonNullable;
    use crate::scalar::Scalar;
    use crate::validity::Validity;

    fn check_groups(
        scalar: Scalar,
        fixed_size: bool,
        options: NumericalAggregateOpts,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let elements = ConstantArray::new(scalar, 8).into_array();
        let decoded = elements
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)?
            .into_array();
        let make_groups = |elements: ArrayRef| -> VortexResult<GroupedArray> {
            if fixed_size {
                Ok(FixedSizeListArray::try_new(
                    elements,
                    2,
                    Validity::from_iter([true, false, true, true]),
                    4,
                )?
                .into())
            } else {
                Ok(ListViewArray::try_new(
                    elements,
                    buffer![0u32, 4, 2, 8, 1].into_array(),
                    buffer![3u32, 2, 4, 0, 0].into_array(),
                    Validity::from_iter([true, false, true, true, true]),
                )?
                .into())
            }
        };
        let groups = make_groups(elements.clone())?;
        let reference = make_groups(decoded.clone())?;
        let as_array = |groups: &GroupedArray| match groups {
            GroupedArray::ListView(array) => array.clone().into_array(),
            GroupedArray::FixedSizeList(array) => array.clone().into_array(),
        };
        let groups_array = as_array(&groups);
        let reference_array = as_array(&reference);

        for aggregate in [Sum.bind(options), SumV2.bind(options)] {
            assert!(
                ConstantGroupedSumKernel
                    .grouped_aggregate(&aggregate, &groups, &mut ctx)?
                    .is_some()
            );
            let mut actual = aggregate.accumulator_grouped(elements.dtype())?;
            actual.accumulate_list(&groups_array, &mut ctx)?;
            let mut expected = aggregate.accumulator_grouped(decoded.dtype())?;
            expected.accumulate_list(&reference_array, &mut ctx)?;
            assert_arrays_eq!(actual.finish()?, expected.finish()?, &mut ctx);
        }

        Ok(())
    }

    #[rstest]
    #[case::i32((-3i32).into())]
    #[case::null(Scalar::null_native::<i32>())]
    #[case::signed_overflow(i64::MAX.into())]
    #[case::negative_zero((-0.0f64).into())]
    fn primitive_groups(
        #[case] scalar: Scalar,
        #[values(false, true)] fixed_size: bool,
    ) -> VortexResult<()> {
        check_groups(scalar, fixed_size, NumericalAggregateOpts::default())
    }

    #[rstest]
    fn nan_groups(#[values(true, false)] skip_nans: bool) -> VortexResult<()> {
        check_groups(f64::NAN.into(), false, NumericalAggregateOpts { skip_nans })
    }

    #[test]
    fn decimal_kernel_declines() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let dtype = DType::Decimal(DecimalDType::new(10, 2), NonNullable);
        let elements = ConstantArray::new(Scalar::zero_value(&dtype), 4).into_array();
        let groups = FixedSizeListArray::try_new(elements, 2, Validity::NonNullable, 2)?.into();

        for aggregate in [
            Sum.bind(NumericalAggregateOpts::default()),
            SumV2.bind(NumericalAggregateOpts::default()),
        ] {
            assert!(
                ConstantGroupedSumKernel
                    .grouped_aggregate(&aggregate, &groups, &mut ctx)?
                    .is_none()
            );
        }

        Ok(())
    }
}
