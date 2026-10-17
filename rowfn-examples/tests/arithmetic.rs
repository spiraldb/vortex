// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int8Array, Int64Array, UInt32Array};
use arrow_schema::Field;
use rowfn_arrow::{ArrowOperand, ArrowHost};
use rowfn_examples::{Add, Divide, NoOptions, PositiveSum, Seven, SevenPlugin};
use rstest::rstest;
use vortex_array::{IntoArray, VortexSessionExecute, array_session, assert_arrays_eq};
use vortex_array::arrays::{BoolArray, PrimitiveArray};
use vortex_array::scalar_fn::{ScalarFnVTable, VecExecutionArgs};
use vortex_array::scalar_fn::session::ScalarFnSessionExt;
use vortex_array::scalar_fn::unstable::rowfn::VortexHost;

use common::{TestResult, errors, pair, run};

#[rstest]
#[case::empty(vec![], vec![], vec![])]
#[case::all_null(vec![i64::MAX, 5], vec![false, false], vec![None, None])]
#[case::partial(vec![i64::MAX, 5, -2], vec![false, true, true], vec![None, Some(6), Some(-1)])]
#[case::all_valid(vec![1, 5, -2], vec![true, true, true], vec![Some(2), Some(6), Some(-1)])]
fn checked_add_conformance(#[case] values: Vec<i64>, #[case] valid: Vec<bool>, #[case] expected: Vec<Option<i64>>) -> TestResult {
    let rows = values.len();
    let (arrow, vortex) = run(&Add::<true>, vec![pair(&values, &valid, false, rows), pair(&[1], &[true], true, rows)], rows)?;
    assert_eq!(arrow.array.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>(), expected);
    let expected = PrimitiveArray::from_option_iter(expected).into_array();
    assert_arrays_eq!(&vortex, &expected, &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn scalar_only_and_zero_logical_rows() -> TestResult {
    for rows in [0, 1, 7] {
        let (arrow, vortex) = run(&Add::<false>, vec![pair(&[i64::MAX], &[true], true, rows), pair(&[1], &[true], true, rows)], rows)?;
        assert_eq!(arrow.array.len(), rows);
        assert_eq!(arrow.array.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(), vec![i64::MIN; rows]);
        assert_eq!(vortex.len(), rows);
    }
    Ok(())
}

#[test]
fn errors_on_valid_rows_are_observable_in_both_hosts() -> TestResult {
    for division in [false, true] {
        let (lhs, vx_lhs) = pair(&[i64::MAX, 8], &[true, true], false, 2);
        let (rhs, vx_rhs) = pair(&[if division { 0 } else { 1 }], &[true], true, 2);
        macro_rules! errors {
            ($function:expr, $text:literal) => {{
                let function = $function;
                let fields = [lhs.dtype.clone(), rhs.dtype.clone()];
                let output = rowfn_arrow::plan(&function, &NoOptions, &fields)?;
                let error = rowfn_arrow::invoke(&function, &NoOptions, &[lhs, rhs], 2, output).err().unwrap();
                assert!(error.to_string().contains($text));
                let inputs = [vx_lhs, vx_rhs];
                let types = inputs.iter().map(|a| a.dtype().clone()).collect::<Vec<_>>();
                let output = rowfn::plan::<VortexHost, _>(&function, &NoOptions, &types)?;
                let error = vortex_array::scalar_fn::unstable::rowfn::execute(&function, &NoOptions, &inputs, 2,
                    output.output_type(), &mut array_session().create_execution_ctx()).err().unwrap();
                assert!(error.to_string().contains($text));
            }};
        }
        if division { errors!(Divide, "integer division by zero or overflow"); }
        else { errors!(Add::<true>, "integer overflow in checked add"); }
    }
    Ok(())
}

#[test]
fn division_skips_invalid_payloads() -> TestResult {
    let (arrow, vortex) = run(&Divide, vec![pair(&[9, 8, 6], &[false, true, true], false, 3),
        pair(&[0, 2, 3], &[true, true, true], false, 3)], 3)?;
    let expected = vec![None, Some(4), Some(2)];
    assert_eq!(arrow.array.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>(), expected);
    assert_arrays_eq!(&vortex, &PrimitiveArray::from_option_iter(expected).into_array(), &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn boolean_retry_suppresses_only_null_payload_overflow() -> TestResult {
    let (arrow, vortex) = run(&PositiveSum::<true>, vec![pair(&[i64::MAX, -3, 5], &[false, true, true], false, 3),
        pair(&[1], &[true], true, 3)], 3)?;
    let expected = vec![None, Some(false), Some(true)];
    assert_eq!(arrow.array.as_any().downcast_ref::<arrow_array::BooleanArray>().unwrap().iter().collect::<Vec<_>>(), expected);
    assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected).into_array(), &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn dispatch_selects_signed_and_unsigned_widths() -> TestResult {
    macro_rules! compare {
        ($native:ty, $array:ty, $lhs:expr, $rhs:expr, $expected:expr) => {{
            let lhs: Vec<$native> = $lhs;
            let rhs: Vec<$native> = $rhs;
            let expected: Vec<$native> = $expected;
            let arrow_lhs: ArrayRef = Arc::new(<$array>::from(lhs.clone()));
            let arrow_rhs: ArrayRef = Arc::new(<$array>::from(rhs.clone()));
            let input = |column: ArrayRef| ArrowOperand {
                dtype: Field::new("input", column.data_type().clone(), false),
                column,
                scalar: false,
            };
            let inputs = vec![
                (input(arrow_lhs), PrimitiveArray::from_iter(lhs).into_array()),
                (input(arrow_rhs), PrimitiveArray::from_iter(rhs).into_array()),
            ];
            let (arrow, vortex) = run(&Divide, inputs, expected.len())?;
            assert_eq!(arrow.array.as_any().downcast_ref::<$array>().unwrap().values().as_ref(), expected);
            assert_arrays_eq!(&vortex, &PrimitiveArray::from_iter(expected).into_array(),
                &mut array_session().create_execution_ctx());
        }};
    }
    compare!(i8, Int8Array, vec![126, -3], vec![2, 2], vec![63, -1]);
    compare!(u32, UInt32Array, vec![9, 17], vec![2, 4], vec![4, 4]);
    Ok(())
}

#[test]
fn length_one_array_is_not_a_scalar() -> TestResult {
    let (mut input, _) = pair(&[3], &[true], false, 4);
    let output = rowfn::plan::<ArrowHost, _>(&Add::<false>, &NoOptions, &[input.dtype.clone(), input.dtype.clone()])?;
    assert!(rowfn_arrow::invoke(&Add::<false>, &NoOptions, &[input.clone(), input.clone()], 4, output.output_type().clone()).is_err());
    input.scalar = true;
    assert_eq!(rowfn_arrow::invoke(&Add::<false>, &NoOptions, &[input.clone(), input], 4, output.output_type().clone())?.array.len(), 4);
    Ok(())
}

#[test]
fn externally_registered_nullary_function() -> TestResult {
    let session = array_session();
    let function = SevenPlugin;
    session.scalar_fns().register(function.clone());
    let metadata = function.serialize(&NoOptions)?.unwrap();
    let plugin = session.scalar_fns().registry().get(&function.id()).unwrap();
    let function = plugin.deserialize(&metadata, &session)?;
    for rows in [0, 5] {
        let result = function.execute(&VecExecutionArgs::new(vec![], rows), &mut session.create_execution_ctx())?;
        assert_arrays_eq!(&result, &PrimitiveArray::from_iter(vec![7i64; rows]).into_array(), &mut session.create_execution_ctx());
        let output = rowfn_arrow::plan(&Seven, &NoOptions, &[])?;
        assert_eq!(rowfn_arrow::invoke(&Seven, &NoOptions, &[], rows, output)?.array.len(), rows);
    }
    Ok(())
}

#[test]
fn null_scalar_suppresses_row_errors_without_losing_output_type() -> TestResult {
    for rows in [0, 3] {
        let (arrow, vortex) = run(&Divide, vec![pair(&[0], &[false], true, rows),
            pair(&vec![0; rows], &vec![true; rows], false, rows)], rows)?;
        assert_eq!(arrow.array.null_count(), rows);
        assert!(arrow.field.is_nullable());
        assert_eq!(arrow.array.data_type(), &arrow_schema::DataType::Int64);
        assert_eq!(vortex.len(), rows);
        assert!(vortex.dtype().is_nullable());
    }
    Ok(())
}

#[test]
fn scalar_representations_and_sliced_primitives_agree() -> TestResult {
    let (arrow, vortex) = pair(&[100, 4, 8, 200], &[true; 4], false, 4);
    let arrow = ArrowOperand { column: arrow.column.slice(1, 2), ..arrow };
    let vortex = vortex.slice(1..3)?;
    let (arrow, vortex) = run(&Add::<true>, vec![(arrow, vortex), pair(&[2], &[true], true, 2)], 2)?;
    assert_eq!(arrow.array.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ref(), &[6, 10]);
    assert_arrays_eq!(&vortex, &PrimitiveArray::from_option_iter([Some(6i64), Some(10)]).into_array(),
        &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn deferred_boolean_modes_report_valid_overflow() -> TestResult {
    macro_rules! check_mode {
        ($mode:literal) => {{
            let valid = vec![true; 65];
            let mut values = vec![1; 65];
            values[64] = i64::MAX;
            let (arrow, vortex) = errors(
                &PositiveSum::<$mode>,
                vec![pair(&values, &valid, false, 65), pair(&[1], &[true], true, 65)],
                65,
            )?;
            for error in [arrow, vortex] {
                assert!(error.contains("integer overflow in positive sum"));
            }
            values[64] = 1;
            let (arrow, vortex) = run(&PositiveSum::<$mode>, vec![pair(&values, &valid, false, 65),
                pair(&[1], &[true], true, 65)], 65)?;
            assert_eq!(arrow.array.len(), 65);
            assert_arrays_eq!(&vortex, &BoolArray::from_iter(vec![Some(true); 65]).into_array(),
                &mut array_session().create_execution_ctx());
        }};
    }
    check_mode!(false);
    check_mode!(true);
    Ok(())
}
