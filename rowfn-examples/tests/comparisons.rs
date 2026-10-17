// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, Datum, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, Scalar, StringArray, StringViewArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field};
use rowfn::RowFn;
use rowfn_arrow::{ArrowHost, ArrowOperand};
use rowfn_examples::{Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NoOptions,
    NotEqual, StringEqual, StringGreaterThan, StringGreaterThanOrEqual, StringLessThan,
    StringLessThanOrEqual, StringNotEqual};
use vortex_array::{IntoArray, VortexSessionExecute, array_session, assert_arrays_eq};
use vortex_array::arrays::{BoolArray, ConstantArray, PrimitiveArray, VarBinViewArray};
use vortex_array::scalar_fn::unstable::rowfn::VortexHost;
use vortex_array::validity::Validity;

use common::{TestResult, pair, run};

#[track_caller]
fn check<F>(function: &F, expected: &[Option<bool>]) -> TestResult
where
    F: RowFn<ArrowHost, Options = NoOptions> + RowFn<VortexHost, Options = NoOptions>,
{
    let (arrow, vortex) = run(
        function,
        vec![
            pair(&[-2, 7, 100, 3], &[true, true, false, true], false, 4),
            pair(&[3, 7, -5, 3], &[true, true, true, false], false, 4),
        ],
        4,
    )?;

    assert_eq!(arrow.field.data_type(), &DataType::Boolean);
    assert_eq!(
        arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap().iter().collect::<Vec<_>>(),
        expected.to_vec(),
    );
    assert_arrays_eq!(
        &vortex,
        &BoolArray::from_iter(expected.iter().copied()).into_array(),
        &mut array_session().create_execution_ctx()
    );

    Ok(())
}

#[track_caller]
fn check_arrow_kernel<F>(
    function: &F,
    kernel: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
    lhs: &(ArrowOperand, vortex_array::ArrayRef),
    rhs: &(ArrowOperand, vortex_array::ArrayRef),
) -> TestResult
where
    F: RowFn<ArrowHost, Options = NoOptions> + RowFn<VortexHost, Options = NoOptions>,
{
    let scalar;
    let right: &dyn Datum = if rhs.0.scalar {
        scalar = Scalar::new(&rhs.0.column);
        &scalar
    } else {
        &rhs.0.column
    };
    let expected = kernel(&lhs.0.column, right)?;
    let (arrow, vortex) = run(function, vec![(*lhs).clone(), (*rhs).clone()], expected.len())?;
    let expected = expected.iter().collect::<Vec<_>>();

    assert_eq!(
        arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap().iter().collect::<Vec<_>>(),
        expected,
    );
    assert_arrays_eq!(
        &vortex,
        &BoolArray::from_iter(expected).into_array(),
        &mut array_session().create_execution_ctx()
    );

    Ok(())
}

#[test]
fn six_strict_comparisons_match_on_both_hosts() -> TestResult {
    check(&Equal, &[Some(false), Some(true), None, None])?;
    check(&NotEqual, &[Some(true), Some(false), None, None])?;
    check(&LessThan, &[Some(true), Some(false), None, None])?;
    check(&LessThanOrEqual, &[Some(true), Some(true), None, None])?;
    check(&GreaterThan, &[Some(false), Some(false), None, None])?;
    check(&GreaterThanOrEqual, &[Some(false), Some(true), None, None])?;

    Ok(())
}

#[test]
fn signed_and_unsigned_widths_use_the_shared_dispatch() -> TestResult {
    macro_rules! check_width {
        ($native:ty, $arrow:ty, $lhs:expr, $rhs:expr, $expected:expr) => {{
            let lhs: Vec<$native> = $lhs;
            let rhs: Vec<$native> = $rhs;
            let input = |values: Vec<$native>| {
                let column: ArrayRef = Arc::new(<$arrow>::from(values.clone()));
                let dtype = Field::new("input", column.data_type().clone(), false);
                (ArrowOperand { column, dtype, scalar: false },
                    PrimitiveArray::from_iter(values).into_array())
            };
            let (arrow, vortex) = run(&LessThan, vec![input(lhs), input(rhs)], 2)?;
            let expected: [bool; 2] = $expected;
            assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap().iter()
                .collect::<Vec<_>>(), expected.map(Some).to_vec());
            assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected).into_array(),
                &mut array_session().create_execution_ctx());
        }};
    }

    check_width!(i8, Int8Array, vec![-3, 2], vec![0, 2], [true, false]);
    check_width!(i16, Int16Array, vec![-3, 2], vec![0, 2], [true, false]);
    check_width!(i32, Int32Array, vec![-3, 2], vec![0, 2], [true, false]);
    check_width!(i64, Int64Array, vec![-3, 2], vec![0, 2], [true, false]);
    check_width!(u8, UInt8Array, vec![0, 2], vec![3, 2], [true, false]);
    check_width!(u16, UInt16Array, vec![0, 2], vec![3, 2], [true, false]);
    check_width!(u32, UInt32Array, vec![0, 2], vec![3, 2], [true, false]);
    check_width!(u64, UInt64Array, vec![0, 2], vec![3, 2], [true, false]);

    Ok(())
}

#[test]
fn scalar_and_empty_batches_preserve_strict_nulls() -> TestResult {
    let (arrow, vortex) = run(
        &LessThan,
        vec![
            pair(&[2, 4, 2], &[true; 3], false, 3),
            pair(&[3], &[true], true, 3),
        ],
        3,
    )?;
    let expected = [Some(true), Some(false), Some(true)];
    assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap().iter()
        .collect::<Vec<_>>(), expected.to_vec());
    assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected).into_array(),
        &mut array_session().create_execution_ctx());

    for rows in [0, 3] {
        let (arrow, vortex) = run(
            &GreaterThan,
            vec![
                pair(&[5], &[false], true, rows),
                pair(&vec![0; rows], &vec![true; rows], false, rows),
            ],
            rows,
        )?;
        assert_eq!(arrow.array.len(), rows);
        assert_eq!(arrow.array.null_count(), rows);
        assert!(arrow.field.is_nullable());
        assert_eq!(vortex.len(), rows);
        assert!(vortex.dtype().is_nullable());
    }

    Ok(())
}

#[test]
fn floating_total_order_matches_arrow_for_nans_and_signed_zero() -> TestResult {
    macro_rules! check_width {
        ($native:ty, $arrow:ty, $lhs:expr, $rhs:expr) => {{
            let lhs: Vec<$native> = $lhs;
            let rhs: Vec<$native> = $rhs;
            let valid = [true, true, true, true, true, true, true, false];
            let input = |values: Vec<$native>| {
                let column: ArrayRef = Arc::new(<$arrow>::new(
                    values.clone().into(),
                    Some(NullBuffer::from(valid.to_vec())),
                ));
                let dtype = Field::new("input", column.data_type().clone(), true);
                let vortex = PrimitiveArray::new(values, Validity::from_iter(valid)).into_array();
                (ArrowOperand { column, dtype, scalar: false }, vortex)
            };
            let lhs = input(lhs);
            let rhs = input(rhs);

            let equal = arrow_ord::cmp::eq(&lhs.0.column, &rhs.0.column)?;
            assert!(!equal.value(0));
            assert!(equal.value(2));
            assert!(!equal.value(3));
            assert!(!equal.is_valid(7));
            let less = arrow_ord::cmp::lt(&lhs.0.column, &rhs.0.column)?;
            assert!(less.value(0));

            check_arrow_kernel(&Equal, arrow_ord::cmp::eq, &lhs, &rhs)?;
            check_arrow_kernel(&NotEqual, arrow_ord::cmp::neq, &lhs, &rhs)?;
            check_arrow_kernel(&LessThan, arrow_ord::cmp::lt, &lhs, &rhs)?;
            check_arrow_kernel(&LessThanOrEqual, arrow_ord::cmp::lt_eq, &lhs, &rhs)?;
            check_arrow_kernel(&GreaterThan, arrow_ord::cmp::gt, &lhs, &rhs)?;
            check_arrow_kernel(&GreaterThanOrEqual, arrow_ord::cmp::gt_eq, &lhs, &rhs)?;
        }};
    }

    check_width!(
        f32,
        Float32Array,
        vec![-0.0, 0.0, f32::from_bits(0x7fc0_0001), f32::from_bits(0x7fc0_0001),
            f32::from_bits(0xffc0_0001), f32::INFINITY, f32::NEG_INFINITY, 9.0],
        vec![0.0, -0.0, f32::from_bits(0x7fc0_0001), f32::from_bits(0x7fc0_0002),
            f32::NEG_INFINITY, f32::NAN, f32::NAN, 9.0]
    );
    check_width!(
        f64,
        Float64Array,
        vec![-0.0, 0.0, f64::from_bits(0x7ff8_0000_0000_0001),
            f64::from_bits(0x7ff8_0000_0000_0001), f64::from_bits(0xfff8_0000_0000_0001),
            f64::INFINITY, f64::NEG_INFINITY, 9.0],
        vec![0.0, -0.0, f64::from_bits(0x7ff8_0000_0000_0001),
            f64::from_bits(0x7ff8_0000_0000_0002), f64::NEG_INFINITY, f64::NAN, f64::NAN, 9.0]
    );

    Ok(())
}

#[test]
fn unknown_float_extensions_and_unsupported_domains_are_rejected() {
    let field = Field::new("input", DataType::Float64, false)
        .with_metadata([("ARROW:extension:name".to_string(), "other".to_string())].into());
    assert!(rowfn_arrow::plan(&Equal, &NoOptions, &[field.clone(), field]).is_err());

    let field = Field::new("input", DataType::Boolean, false);
    assert!(rowfn_arrow::plan(&Equal, &NoOptions, &[field.clone(), field]).is_err());
}

fn strings(values: &[Option<&str>], layout: usize, scalar: bool, rows: usize)
    -> (ArrowOperand, vortex_array::ArrayRef)
{
    let column: ArrayRef = match layout {
        0 => Arc::new(StringArray::from(values.to_vec())),
        1 => Arc::new(LargeStringArray::from(values.to_vec())),
        _ => Arc::new(StringViewArray::from(values.to_vec())),
    };
    let dtype = Field::new("input", column.data_type().clone(), true);
    let vortex = if scalar {
        ConstantArray::new(values[0], rows).into_array()
    } else {
        VarBinViewArray::from_iter_nullable_str(values.iter().copied()).into_array()
    };

    (ArrowOperand { column, dtype, scalar }, vortex)
}

#[test]
fn string_comparisons_match_arrow_across_layouts_and_slices() -> TestResult {
    let left = [
        Some("outside"),
        Some("éclair"),
        Some("a long common prefix ending in α"),
        None,
        Some("same"),
        Some("discard"),
    ];
    let right = [
        Some("outside"),
        Some("eclair"),
        Some("a long common prefix ending in β"),
        Some("ignored"),
        Some("same"),
        Some("discard"),
    ];

    for layout in 0..3 {
        let mut lhs = strings(&left, layout, false, left.len());
        lhs.0.column = lhs.0.column.slice(1, 4);
        lhs.1 = lhs.1.slice(1..5)?;
        let mut rhs = strings(&right, layout, false, right.len());
        rhs.0.column = rhs.0.column.slice(1, 4);
        rhs.1 = rhs.1.slice(1..5)?;

        check_arrow_kernel(&StringEqual, arrow_ord::cmp::eq, &lhs, &rhs)?;
        check_arrow_kernel(&StringNotEqual, arrow_ord::cmp::neq, &lhs, &rhs)?;
        check_arrow_kernel(&StringLessThan, arrow_ord::cmp::lt, &lhs, &rhs)?;
        check_arrow_kernel(&StringLessThanOrEqual, arrow_ord::cmp::lt_eq, &lhs, &rhs)?;
        check_arrow_kernel(&StringGreaterThan, arrow_ord::cmp::gt, &lhs, &rhs)?;
        check_arrow_kernel(&StringGreaterThanOrEqual, arrow_ord::cmp::gt_eq, &lhs, &rhs)?;
    }

    Ok(())
}

#[test]
fn string_scalar_comparisons_match_arrow() -> TestResult {
    let values = [Some("λ"), Some("Ω"), None, Some("λ"), Some("a long string after λ")];

    for layout in 0..3 {
        let lhs = strings(&values, layout, false, values.len());
        let rhs = strings(&[Some("λ")], layout, true, values.len());

        check_arrow_kernel(&StringEqual, arrow_ord::cmp::eq, &lhs, &rhs)?;
        check_arrow_kernel(&StringLessThan, arrow_ord::cmp::lt, &lhs, &rhs)?;
        check_arrow_kernel(&StringGreaterThan, arrow_ord::cmp::gt, &lhs, &rhs)?;
    }

    Ok(())
}

#[test]
fn string_comparisons_reject_unknown_extensions_and_mixed_arrow_layouts() {
    let utf8 = Field::new("input", DataType::Utf8, false);
    let large = Field::new("input", DataType::LargeUtf8, false);
    assert!(rowfn_arrow::plan(&StringEqual, &NoOptions, &[utf8.clone(), large]).is_err());

    let extension = utf8.clone()
        .with_metadata([("ARROW:extension:name".to_string(), "other".to_string())].into());
    assert!(rowfn_arrow::plan(&StringLessThan, &NoOptions, &[utf8, extension]).is_err());
}
