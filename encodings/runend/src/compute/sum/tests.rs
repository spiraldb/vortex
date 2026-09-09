// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::AggregateFnVTableExt;
use vortex_array::aggregate_fn::GroupedArray;
use vortex_array::aggregate_fn::NumericalAggregateOpts;
use vortex_array::aggregate_fn::fns::sum::Sum;
use vortex_array::aggregate_fn::fns::sum_v2::SumV2;
use vortex_array::aggregate_fn::kernels::DynAggregateKernel;
use vortex_array::aggregate_fn::kernels::DynGroupedAggregateKernel;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::FixedSizeListArray;
use vortex_array::arrays::ListViewArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::assert_arrays_eq;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::Nullability::NonNullable;
use vortex_array::dtype::PType;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_buffer::buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use super::RunEndSumKernel;
use crate::RunEnd;
use crate::tests::SESSION;

/// Compare registered dispatch and the direct kernel with a decoded primitive reference.
fn check_sum(array: ArrayRef, options: NumericalAggregateOpts) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let decoded = array
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)?
        .into_array();

    for aggregate in [Sum.bind(options), SumV2.bind(options)] {
        let mut reference = aggregate.accumulator(array.dtype())?;
        reference.accumulate(&decoded, &mut ctx)?;
        let expected = reference.finish()?;
        let partial = RunEndSumKernel
            .aggregate(&aggregate, &array, &mut ctx)?
            .ok_or_else(|| vortex_err!("Primitive run-end kernel declined"))?;
        let mut direct = aggregate.accumulator(array.dtype())?;
        direct.combine_partials(partial)?;
        let mut dispatched = aggregate.accumulator(array.dtype())?;
        dispatched.accumulate(&array, &mut ctx)?;

        for actual in [direct.finish()?, dispatched.finish()?] {
            if expected.as_primitive().is_nan() {
                assert!(actual.as_primitive().is_nan());
            } else {
                assert_eq!(actual, expected);
            }
        }
    }

    Ok(())
}

/// Compare the registered grouped kernels with groups over decoded primitive elements.
fn check_groups(
    groups: GroupedArray,
    reference: GroupedArray,
    options: NumericalAggregateOpts,
) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let as_array = |groups: &GroupedArray| match groups {
        GroupedArray::ListView(array) => array.clone().into_array(),
        GroupedArray::FixedSizeList(array) => array.clone().into_array(),
    };
    let groups_array = as_array(&groups);
    let reference_array = as_array(&reference);

    for aggregate in [Sum.bind(options), SumV2.bind(options)] {
        assert!(
            RunEndSumKernel
                .grouped_aggregate(&aggregate, &groups, &mut ctx)?
                .is_some()
        );
        let mut expected = aggregate.accumulator_grouped(reference.elements().dtype())?;
        expected.accumulate_list(&reference_array, &mut ctx)?;
        let mut actual = aggregate.accumulator_grouped(groups.elements().dtype())?;
        actual.accumulate_list(&groups_array, &mut ctx)?;
        assert_arrays_eq!(actual.finish()?, expected.finish()?, &mut ctx);
    }

    Ok(())
}

#[rstest]
fn primitive_types(
    #[values(PType::U8, PType::U16, PType::U32, PType::U64)] ends_type: PType,
    #[values(
        PType::U8, PType::U16, PType::U32, PType::U64, PType::I8, PType::I16, PType::I32,
        PType::I64, PType::F16, PType::F32, PType::F64
    )]
    values_type: PType,
) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let ends = buffer![2u64, 5, 9]
        .into_array()
        .cast(DType::Primitive(ends_type, NonNullable))?;
    let values = buffer![1u64, 3, 7]
        .into_array()
        .cast(DType::Primitive(values_type, NonNullable))?;
    let array = RunEnd::try_new_offset_length(ends, values, 1, 7, &mut ctx)?.into_array();

    check_sum(array, NumericalAggregateOpts::default())
}

#[rstest]
#[case::all_valid(Validity::AllValid)]
#[case::all_null(Validity::AllInvalid)]
#[case::partially_valid(Validity::from_iter([true, false, true]))]
fn nullable_runs(#[case] validity: Validity) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let values = PrimitiveArray::new(buffer![-3i32, 100, 7], validity).into_array();
    let array = RunEnd::try_new(buffer![2u16, 5, 9].into_array(), values, &mut ctx)?.into_array();

    check_sum(array, NumericalAggregateOpts::default())
}

#[rstest]
#[case::overflow(vec![i64::MAX, 1, -1], vec![1u64, 3, 4])]
#[case::underflow(vec![i64::MIN, -1, 1], vec![1u64, 3, 4])]
#[case::positive_cancellation(vec![-i64::MAX, i64::MAX], vec![1u64, 3])]
#[case::negative_cancellation(vec![i64::MAX, -i64::MAX], vec![1u64, 3])]
fn signed_overflow(#[case] values: Vec<i64>, #[case] ends: Vec<u64>) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let array = RunEnd::try_new(
        PrimitiveArray::from_iter(ends).into_array(),
        PrimitiveArray::from_iter(values).into_array(),
        &mut ctx,
    )?
    .into_array();

    check_sum(array, NumericalAggregateOpts::default())
}

#[rstest]
#[case::product(buffer![u64::MAX].into_array(), buffer![2u64].into_array())]
#[case::addition(buffer![u64::MAX, 1].into_array(), buffer![1u64, 2].into_array())]
fn unsigned_overflow(#[case] values: ArrayRef, #[case] ends: ArrayRef) -> VortexResult<()> {
    let array = RunEnd::try_new(ends, values, &mut SESSION.create_execution_ctx())?.into_array();

    check_sum(array, NumericalAggregateOpts::default())
}

#[rstest]
#[case::nan(buffer![f64::NAN, 1.25, 2.5].into_array())]
#[case::all_nan(buffer![f64::NAN, f64::NAN, f64::NAN].into_array())]
#[case::infinities(buffer![f64::INFINITY, f64::NEG_INFINITY, 2.5].into_array())]
fn floats(#[case] values: ArrayRef, #[values(true, false)] skip_nans: bool) -> VortexResult<()> {
    let array = RunEnd::try_new(
        buffer![2u64, 4, 7].into_array(),
        values,
        &mut SESSION.create_execution_ctx(),
    )?
    .into_array();

    check_sum(array, NumericalAggregateOpts { skip_nans })
}

#[rstest]
fn float_run_product_cancellation(
    #[values(1e308, -1e308)] value: f64,
    #[values(false, true)] grouped: bool,
    #[values(false, true)] skip_nans: bool,
) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let array = RunEnd::try_new(
        buffer![1u64, 3].into_array(),
        buffer![-value, value].into_array(),
        &mut ctx,
    )?
    .into_array();

    if !grouped {
        return check_sum(array, NumericalAggregateOpts { skip_nans });
    }

    let groups =
        FixedSizeListArray::try_new(array.clone(), 3, Validity::NonNullable, 1)?.into_array();
    let expected = PrimitiveArray::from_option_iter([Some(value)]).into_array();
    for aggregate in [
        Sum.bind(NumericalAggregateOpts { skip_nans }),
        SumV2.bind(NumericalAggregateOpts { skip_nans }),
    ] {
        let mut acc = aggregate.accumulator_grouped(array.dtype())?;
        acc.accumulate_list(&groups, &mut ctx)?;
        assert_arrays_eq!(acc.finish()?, expected, &mut ctx);
    }

    Ok(())
}

#[test]
fn empty_and_zero_length_runs() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    // Zero-length boundary runs must not contribute a NaN or an overflow.
    let array = RunEnd::try_new_offset_length(
        buffer![2u64, 5, 8].into_array(),
        buffer![f64::NAN, 3.0, f64::INFINITY].into_array(),
        2,
        3,
        &mut ctx,
    )?
    .into_array();
    check_sum(array, NumericalAggregateOpts::include_nans())?;

    let empty = RunEnd::try_new(
        PrimitiveArray::from_iter(Vec::<u64>::new()).into_array(),
        PrimitiveArray::from_iter(Vec::<i32>::new()).into_array(),
        &mut ctx,
    )?
    .into_array();
    check_sum(empty, NumericalAggregateOpts::default())?;

    let retained = RunEnd::try_new_offset_length(
        buffer![2u64].into_array(),
        buffer![f64::NAN].into_array(),
        0,
        0,
        &mut ctx,
    )?
    .into_array();
    check_sum(retained, NumericalAggregateOpts::include_nans())
}

#[test]
#[cfg(target_pointer_width = "64")]
fn huge_run_uses_registered_kernel() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let len = usize::try_from(i64::MAX)? + 1;
    let array = RunEnd::try_new(
        buffer![len as u64].into_array(),
        buffer![-1i64].into_array(),
        &mut ctx,
    )?
    .into_array();

    for aggregate in [
        Sum.bind(NumericalAggregateOpts::default()),
        SumV2.bind(NumericalAggregateOpts::default()),
    ] {
        let mut acc = aggregate.accumulator(array.dtype())?;
        acc.accumulate(&array, &mut ctx)?;
        assert_eq!(acc.finish()?, Scalar::from(i64::MIN));
    }

    Ok(())
}

#[rstest]
#[case::nullable(PrimitiveArray::from_option_iter([Some(3i32), None, Some(5)]).into_array())]
#[case::overflow(buffer![u64::MAX, 1, 2].into_array())]
#[case::floats(buffer![f64::INFINITY, f64::NEG_INFINITY, f64::NAN].into_array())]
fn grouped_sums(
    #[case] values: ArrayRef,
    #[values(false, true)] fixed_size: bool,
    #[values(false, true)] skip_nans: bool,
) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let elements =
        RunEnd::try_new_offset_length(buffer![3u32, 7, 12].into_array(), values, 1, 10, &mut ctx)?
            .into_array();
    let decoded = elements
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)?
        .into_array();
    let make_groups = |values| -> VortexResult<GroupedArray> {
        if fixed_size {
            Ok(FixedSizeListArray::try_new(values, 2, Validity::NonNullable, 5)?.into())
        } else {
            Ok(ListViewArray::try_new(
                values,
                buffer![6u32, 0, 3, 2, 10].into_array(),
                buffer![3u32, 5, 4, 0, 0].into_array(),
                Validity::from_iter([true, false, true, true, true]),
            )?
            .into())
        }
    };
    check_groups(
        make_groups(elements)?,
        make_groups(decoded)?,
        NumericalAggregateOpts { skip_nans },
    )
}

#[rstest]
#[case::empty(0, 0, false)]
#[case::single_elements(0, 1, false)]
#[case::short_groups(0, 2, false)]
#[case::sliced_run(1, 2, false)]
#[case::run_boundary(3, 2, false)]
#[case::null_groups(1, 8, true)]
#[case::multiple_runs(0, 8, false)]
fn consecutive_groups(
    #[case] offset: usize,
    #[case] size: u32,
    #[case] null_groups: bool,
    #[values(
        buffer![1i64, -2, 3, -4, 5].into_array(),
        PrimitiveArray::from_option_iter([None, Some(2i64), None, Some(4), Some(5)]).into_array(),
        PrimitiveArray::from_option_iter([None::<i64>; 5]).into_array(),
        buffer![i64::MAX, 1, -2, i64::MIN, 3].into_array()
    )]
    values: ArrayRef,
) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let elements = RunEnd::try_new_offset_length(
        buffer![3u32, 7, 10, 17, 64].into_array(),
        values,
        offset,
        size as usize * 6,
        &mut ctx,
    )?
    .into_array();
    let decoded = elements
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)?
        .into_array();
    let validity = if null_groups {
        Validity::from_iter([true, false, true, false, true, true])
    } else {
        Validity::NonNullable
    };

    check_groups(
        FixedSizeListArray::try_new(elements, size, validity.clone(), 6)?.into(),
        FixedSizeListArray::try_new(decoded, size, validity, 6)?.into(),
        NumericalAggregateOpts::default(),
    )
}

#[test]
fn decimal_kernels_decline() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let values = DecimalArray::new(
        buffer![100i64, 200],
        DecimalDType::new(10, 2),
        Validity::NonNullable,
    )
    .into_array();
    let array = RunEnd::try_new(buffer![2u64, 4].into_array(), values, &mut ctx)?.into_array();
    let groups = FixedSizeListArray::try_new(array.clone(), 2, Validity::NonNullable, 2)?.into();

    for aggregate in [
        Sum.bind(NumericalAggregateOpts::default()),
        SumV2.bind(NumericalAggregateOpts::default()),
    ] {
        assert!(
            RunEndSumKernel
                .aggregate(&aggregate, &array, &mut ctx)?
                .is_none()
        );
        assert!(
            RunEndSumKernel
                .grouped_aggregate(&aggregate, &groups, &mut ctx)?
                .is_none()
        );
    }

    Ok(())
}
