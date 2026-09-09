// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Primitive sums over constant elements.
//!
//! Whole-array sums already multiply the scalar by its length. Grouped sums apply the same
//! arithmetic to each group size without decoding the elements or slicing a constant per group.

use vortex_buffer::BitBufferMut;
use vortex_error::VortexResult;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::aggregate_fn::AggregateFnRef;
use crate::aggregate_fn::GroupedArray;
use crate::aggregate_fn::fns::sum::Sum;
use crate::aggregate_fn::fns::sum::multiply_constant;
use crate::aggregate_fn::fns::sum_v2::SumV2;
use crate::aggregate_fn::kernels::DynGroupedAggregateKernel;
use crate::arrays::Constant;
use crate::builders::builder_with_capacity_in;
use crate::scalar::Scalar;

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
        let Some(options) = aggregate_fn
            .as_opt::<Sum>()
            .or_else(|| aggregate_fn.as_opt::<SumV2>())
        else {
            return Ok(None);
        };
        let Some(elements) = groups.elements().as_opt::<Constant>() else {
            return Ok(None);
        };
        if !elements.dtype().is_primitive() {
            return Ok(None);
        }

        let Some(sum_dtype) = aggregate_fn.return_dtype(elements.dtype()) else {
            return Ok(None);
        };
        let ranges = groups.group_ranges(ctx)?;
        let validity = groups.group_validity(ctx)?;
        let scalar = elements.scalar();
        let skip_nan = options.skip_nans && scalar.as_primitive().is_nan();
        let zero = Scalar::zero_value(&sum_dtype);
        let mut sums = builder_with_capacity_in(&sum_dtype, groups.len(), ctx.allocator());
        let mut empty_groups = BitBufferMut::new_unset(groups.len());

        for (index, ((_, size), valid)) in ranges.iter().zip(validity.iter()).enumerate() {
            if !valid {
                sums.append_null();
                continue;
            }

            let is_empty = size == 0 || scalar.is_null();
            let sum = if is_empty || skip_nan {
                zero.clone()
            } else {
                multiply_constant(scalar, size, &sum_dtype)?.unwrap_or_else(|| zero.clone())
            };
            sums.append_scalar(&sum)?;
            empty_groups.set_to(index, is_empty);
        }

        let sums = sums.finish();
        if aggregate_fn.is::<SumV2>() {
            Ok(Some(SumV2::partials_from_sums(
                sums,
                empty_groups.freeze(),
                validity,
                ctx,
            )?))
        } else {
            Ok(Some(sums))
        }
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
    use crate::dtype::half::f16;
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
                Ok(FixedSizeListArray::try_new(elements, 2, Validity::NonNullable, 4)?.into())
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
    #[case::u8(3u8.into())]
    #[case::u16(3u16.into())]
    #[case::u32(3u32.into())]
    #[case::u64(3u64.into())]
    #[case::i8((-3i8).into())]
    #[case::i16((-3i16).into())]
    #[case::i32((-3i32).into())]
    #[case::i64((-3i64).into())]
    #[case::f16(f16::from_f32(1.25).into())]
    #[case::f32(1.25f32.into())]
    #[case::f64(1.25f64.into())]
    #[case::null(Scalar::null_native::<i32>())]
    #[case::signed_overflow(i64::MAX.into())]
    #[case::unsigned_overflow(u64::MAX.into())]
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

    #[rstest]
    #[cfg(target_pointer_width = "64")]
    #[case::zero(0i64, 0i64)]
    #[case::negative_one(-1i64, i64::MIN)]
    fn huge_constants(#[case] value: i64, #[case] expected: i64) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let len = usize::try_from(i64::MAX)? + 1;
        let array = ConstantArray::new(value, len).into_array();
        let groups = ListViewArray::try_new(
            array.clone(),
            buffer![0u64].into_array(),
            buffer![len as u64].into_array(),
            Validity::NonNullable,
        )?
        .into_array();

        for aggregate in [
            Sum.bind(NumericalAggregateOpts::default()),
            SumV2.bind(NumericalAggregateOpts::default()),
        ] {
            let mut acc = aggregate.accumulator(array.dtype())?;
            acc.accumulate(&array, &mut ctx)?;
            assert_eq!(acc.finish()?, Scalar::from(expected));
            let mut grouped = aggregate.accumulator_grouped(array.dtype())?;
            grouped.accumulate_list(&groups, &mut ctx)?;
            assert_arrays_eq!(
                grouped.finish()?,
                PrimitiveArray::from_option_iter([Some(expected)]).into_array(),
                &mut ctx
            );
        }

        Ok(())
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
