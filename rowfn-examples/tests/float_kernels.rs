// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float32Array, Float64Array};
use arrow_schema::Field;
use rowfn_arrow::ArrowOperand;
use rowfn_examples::{FloatArithmetic, FloatNegate, NoOptions};
use vortex_array::{ArrayRef as VortexArrayRef, IntoArray, VortexSessionExecute, array_session, assert_arrays_eq};
use vortex_array::arrays::{ConstantArray, PrimitiveArray};

use common::{TestResult, pair, run};

#[test]
fn binary32_arithmetic_matches_strict_null_semantics() -> TestResult {
    let lhs = f32_pair(&[Some(6.0), Some(-3.0), None], false, 3);
    let rhs = f32_pair(&[Some(2.0)], true, 3);
    for (operation, expected) in [
        (FloatArithmetic::Add, [Some(8.0), Some(-1.0), None]),
        (FloatArithmetic::Subtract, [Some(4.0), Some(-5.0), None]),
        (FloatArithmetic::Multiply, [Some(12.0), Some(-6.0), None]),
        (FloatArithmetic::Divide, [Some(3.0), Some(-1.5), None]),
        (FloatArithmetic::Remainder, [Some(0.0), Some(-1.0), None]),
    ] {
        let (arrow, vortex) = run(&operation, vec![lhs.clone(), rhs.clone()], 3)?;
        assert_eq!(
            arrow.array.as_any().downcast_ref::<Float32Array>().unwrap(),
            &Float32Array::from(expected.to_vec()),
        );
        assert_arrays_eq!(
            &vortex,
            &PrimitiveArray::from_option_iter(expected).into_array(),
            &mut array_session().create_execution_ctx()
        );
    }
    Ok(())
}

#[test]
fn binary64_arithmetic_uses_native_width_and_scalar_broadcast() -> TestResult {
    let (arrow, vortex) = run(
        &FloatArithmetic::Multiply,
        vec![f64_pair(&[Some(1.25), None, Some(-2.0)], false, 3),
             f64_pair(&[Some(4.0)], true, 3)],
        3,
    )?;
    let expected = [Some(5.0f64), None, Some(-8.0)];
    assert_eq!(arrow.array.as_any().downcast_ref::<Float64Array>().unwrap(),
        &Float64Array::from(expected.to_vec()));
    assert_arrays_eq!(&vortex, &PrimitiveArray::from_option_iter(expected).into_array(),
        &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn zero_division_and_remainder_produce_ieee_values() -> TestResult {
    let lhs = f64_pair(&[Some(1.0), Some(0.0), Some(-1.0)], false, 3);
    let rhs = f64_pair(&[Some(0.0)], true, 3);
    let (arrow, vortex) = run(&FloatArithmetic::Divide, vec![lhs.clone(), rhs.clone()], 3)?;
    let arrow = arrow.array.as_any().downcast_ref::<Float64Array>().unwrap();
    let mut ctx = array_session().create_execution_ctx();
    let vortex = vortex.execute::<PrimitiveArray>(&mut ctx)?;
    for values in [arrow.values().as_ref(), vortex.as_slice::<f64>()] {
        assert_eq!(values[0], f64::INFINITY);
        assert!(values[1].is_nan());
        assert_eq!(values[2], f64::NEG_INFINITY);
    }

    let (arrow, vortex) = run(&FloatArithmetic::Remainder, vec![lhs, rhs], 3)?;
    let arrow = arrow.array.as_any().downcast_ref::<Float64Array>().unwrap();
    let vortex = vortex.execute::<PrimitiveArray>(&mut ctx)?;
    assert!(arrow.values().iter().all(|value| value.is_nan()));
    assert!(vortex.as_slice::<f64>().iter().all(|value| value.is_nan()));
    Ok(())
}

#[test]
fn float_negation_preserves_signed_zero_and_nulls() -> TestResult {
    let (arrow, vortex) = run(
        &FloatNegate,
        vec![f32_pair(&[Some(0.0), Some(-0.0), None, Some(f32::INFINITY)], false, 4)],
        4,
    )?;
    let arrow = arrow.array.as_any().downcast_ref::<Float32Array>().unwrap();
    let mut ctx = array_session().create_execution_ctx();
    let vortex = vortex.execute::<PrimitiveArray>(&mut ctx)?;
    for values in [arrow.values().as_ref(), vortex.as_slice::<f32>()] {
        assert_eq!(values[0].to_bits(), (-0.0f32).to_bits());
        assert_eq!(values[1].to_bits(), 0.0f32.to_bits());
        assert_eq!(values[3], f32::NEG_INFINITY);
    }
    assert!(arrow.is_null(2));
    assert!(vortex.dtype().is_nullable());
    Ok(())
}

#[test]
fn float_kernels_keep_empty_and_all_null_output_types() -> TestResult {
    for rows in [0, 3] {
        let (arrow, vortex) = run(
            &FloatArithmetic::Add,
            vec![f32_pair(&vec![None; rows], false, rows), f32_pair(&[Some(1.0)], true, rows)],
            rows,
        )?;
        assert_eq!(arrow.array.data_type(), &arrow_schema::DataType::Float32);
        assert_eq!(arrow.array.null_count(), rows);
        assert_eq!(vortex.len(), rows);
    }
    Ok(())
}

#[test]
fn float_dispatch_rejects_integers_and_mixed_widths() -> TestResult {
    let (integer, _) = pair(&[1], &[true], false, 1);
    assert!(rowfn_arrow::plan(&FloatArithmetic::Add, &NoOptions,
        &[integer.dtype.clone(), integer.dtype]).is_err());

    let (f32_input, _) = f32_pair(&[Some(1.0)], false, 1);
    let (f64_input, _) = f64_pair(&[Some(2.0)], false, 1);
    assert!(rowfn_arrow::plan(&FloatArithmetic::Add, &NoOptions,
        &[f32_input.dtype, f64_input.dtype]).is_err());
    Ok(())
}

fn f32_pair(values: &[Option<f32>], scalar: bool, rows: usize) -> (ArrowOperand, VortexArrayRef) {
    let column = Arc::new(Float32Array::from(values.to_vec())) as ArrayRef;
    let vortex = if scalar {
        ConstantArray::new(values[0], rows).into_array()
    } else {
        PrimitiveArray::from_option_iter(values.iter().copied()).into_array()
    };
    let dtype = Field::new("input", column.data_type().clone(), true);
    (ArrowOperand { column, dtype, scalar }, vortex)
}

fn f64_pair(values: &[Option<f64>], scalar: bool, rows: usize) -> (ArrowOperand, VortexArrayRef) {
    let column = Arc::new(Float64Array::from(values.to_vec())) as ArrayRef;
    let vortex = if scalar {
        ConstantArray::new(values[0], rows).into_array()
    } else {
        PrimitiveArray::from_option_iter(values.iter().copied()).into_array()
    };
    let dtype = Field::new("input", column.data_type().clone(), true);
    (ArrowOperand { column, dtype, scalar }, vortex)
}
