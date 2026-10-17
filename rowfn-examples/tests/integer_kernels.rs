// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::Field;
use rowfn_arrow::ArrowOperand;
use rowfn_examples::{
    BitwiseAnd, BitwiseAndNot, BitwiseNot, BitwiseOr, BitwiseXor, MultiplyWrapping,
    Negate, Remainder, ShiftLeft, ShiftRight, Subtract,
};
use vortex_array::{IntoArray, VortexSessionExecute, array_session, assert_arrays_eq};
use vortex_array::arrays::PrimitiveArray;

use common::{TestResult, errors, pair, run};

#[test]
fn integer_arithmetic_preserves_values_and_nulls() -> TestResult {
    let left = pair(&[i64::MIN, 21, 8, 7], &[true, true, false, true], false, 4);
    let right = pair(&[-1, 3, 0, 0], &[true, true, true, false], false, 4);

    let (arrow, vortex) = run(&Subtract::<false>, vec![left.clone(), right.clone()], 4)?;
    assert_values(&arrow, &vortex, [Some(i64::MIN.wrapping_sub(-1)), Some(18), None, None]);

    let (arrow, vortex) = run(&MultiplyWrapping, vec![left.clone(), right.clone()], 4)?;
    assert_values(&arrow, &vortex, [Some(i64::MIN), Some(63), None, None]);

    let (arrow, vortex) = run(&Remainder, vec![left, right], 4)?;
    assert_values(&arrow, &vortex, [Some(0), Some(0), None, None]);
    Ok(())
}

#[test]
fn integer_errors_only_come_from_valid_rows() -> TestResult {
    let cases = [
        (true, "integer overflow in checked subtract"),
        (false, "integer remainder by zero"),
    ];
    for (subtract, message) in cases {
        let inputs = vec![
            pair(&[i64::MIN, 7], &[true, true], false, 2),
            pair(&[1, 0], &[true, true], false, 2),
        ];
        let (arrow, vortex) = if subtract {
            errors(&Subtract::<true>, inputs, 2)?
        } else {
            errors(&Remainder, inputs, 2)?
        };
        assert!(arrow.contains(message));
        assert!(vortex.contains(message));
    }

    let (arrow, vortex) = run(
        &Subtract::<true>,
        vec![pair(&[i64::MIN, 7], &[false, true], false, 2), pair(&[1], &[true], true, 2)],
        2,
    )?;
    assert_values(&arrow, &vortex, [None, Some(6)]);

    let (arrow, vortex) = run(
        &Remainder,
        vec![pair(&[7, 8], &[false, true], false, 2), pair(&[0, 2], &[true, true], false, 2)],
        2,
    )?;
    assert_values(&arrow, &vortex, [None, Some(0)]);
    Ok(())
}

#[test]
fn negate_matches_signed_and_unsigned_arrow_domains() -> TestResult {
    let (arrow, vortex) = run(&Negate::<false>, vec![pair(&[i64::MIN, 2], &[true, false], false, 2)], 2)?;
    assert_values(&arrow, &vortex, [Some(i64::MIN), None]);

    let (arrow, vortex) = errors(&Negate::<true>, vec![pair(&[i64::MIN], &[true], false, 1)], 1)?;
    assert!(arrow.contains("integer overflow in checked negate"));
    assert!(vortex.contains("integer overflow in checked negate"));

    let (arrow, vortex) = run(&Negate::<true>, vec![pair(&[i64::MIN, 2], &[false, true], false, 2)], 2)?;
    assert_values(&arrow, &vortex, [None, Some(-2)]);

    let unsigned = Arc::new(UInt32Array::from(vec![1u32])) as ArrayRef;
    let unsigned_input = ArrowOperand {
        dtype: Field::new("value", unsigned.data_type().clone(), false),
        column: unsigned,
        scalar: false,
    };
    let vortex = PrimitiveArray::from_iter([1u32]).into_array();
    let (arrow, vortex) = run(&Negate::<false>, vec![(unsigned_input, vortex)], 1)?;
    assert_eq!(arrow.array.as_any().downcast_ref::<UInt32Array>().unwrap().value(0), u32::MAX);
    assert_arrays_eq!(
        &vortex,
        &PrimitiveArray::from_iter([u32::MAX]).into_array(),
        &mut array_session().create_execution_ctx()
    );

    let values = Arc::new(UInt32Array::from(vec![1u32])) as ArrayRef;
    let input = ArrowOperand {
        dtype: Field::new("value", values.data_type().clone(), false),
        column: values,
        scalar: false,
    };
    assert!(rowfn_arrow::plan(&Negate::<true>, &rowfn_examples::NoOptions, &[input.dtype]).is_err());
    Ok(())
}

#[test]
fn bitwise_operations_preserve_nulls_and_scalar_broadcast() -> TestResult {
    let left = pair(&[0b1100, 0b1010, -1], &[true, true, false], false, 3);
    let right = pair(&[0b1010], &[true], true, 3);

    let (arrow, vortex) = run(&BitwiseAnd, vec![left.clone(), right.clone()], 3)?;
    assert_values(&arrow, &vortex, [Some(0b1000), Some(0b1010), None]);
    let (arrow, vortex) = run(&BitwiseOr, vec![left.clone(), right.clone()], 3)?;
    assert_values(&arrow, &vortex, [Some(0b1110), Some(0b1010), None]);
    let (arrow, vortex) = run(&BitwiseXor, vec![left.clone(), right.clone()], 3)?;
    assert_values(&arrow, &vortex, [Some(0b0110), Some(0), None]);
    let (arrow, vortex) = run(&BitwiseAndNot, vec![left.clone(), right], 3)?;
    assert_values(&arrow, &vortex, [Some(0b0100), Some(0), None]);
    let (arrow, vortex) = run(&BitwiseNot, vec![left], 3)?;
    assert_values(&arrow, &vortex, [Some(!0b1100), Some(!0b1010), None]);
    Ok(())
}

#[test]
fn shifts_wrap_counts_and_keep_signed_right_shift() -> TestResult {
    let left = pair(&[1, -8, 9], &[true, true, false], false, 3);
    let right = pair(&[65, -1, 1], &[true, true, true], false, 3);
    let (arrow, vortex) = run(&ShiftLeft, vec![left.clone(), right.clone()], 3)?;
    assert_values(&arrow, &vortex, [Some(2), Some((-8i64).wrapping_shl(63)), None]);
    let (arrow, vortex) = run(&ShiftRight, vec![left, right], 3)?;
    assert_values(&arrow, &vortex, [Some(0), Some(-1), None]);
    Ok(())
}

#[test]
fn dispatch_covers_signed_and_unsigned_widths() -> TestResult {
    macro_rules! check {
        ($native:ty, $array:ty, $value:expr) => {{
            let value: $native = $value;
            let column = Arc::new(<$array>::from(vec![value])) as ArrayRef;
            let input = ArrowOperand {
                dtype: Field::new("value", column.data_type().clone(), false),
                column,
                scalar: false,
            };
            let vortex = PrimitiveArray::from_iter([value]).into_array();
            let (arrow, vortex) = run(&BitwiseNot, vec![(input, vortex)], 1)?;
            assert_eq!(arrow.array.as_any().downcast_ref::<$array>().unwrap().value(0), !value);
            assert_arrays_eq!(
                &vortex,
                &PrimitiveArray::from_iter([!value]).into_array(),
                &mut array_session().create_execution_ctx()
            );
        }};
    }
    check!(i8, Int8Array, -7);
    check!(i16, Int16Array, -7);
    check!(i32, Int32Array, -7);
    check!(i64, Int64Array, -7);
    check!(u8, UInt8Array, 7);
    check!(u16, UInt16Array, 7);
    check!(u32, UInt32Array, 7);
    check!(u64, UInt64Array, 7);
    Ok(())
}

#[test]
fn empty_and_all_null_batches_preserve_integer_output() -> TestResult {
    for rows in [0, 3] {
        let (arrow, vortex) = run(
            &Remainder,
            vec![
                pair(&vec![7; rows], &vec![false; rows], false, rows),
                pair(&[0], &[true], true, rows),
            ],
            rows,
        )?;
        assert_eq!(arrow.array.data_type(), &arrow_schema::DataType::Int64);
        assert_eq!(arrow.array.null_count(), rows);
        assert_eq!(vortex.len(), rows);
        if rows > 0 {
            assert!(vortex.dtype().is_nullable());
        }
    }
    Ok(())
}

fn assert_values<const N: usize>(
    arrow: &rowfn_arrow::Output,
    vortex: &vortex_array::ArrayRef,
    expected: [Option<i64>; N],
) {
    assert_eq!(
        arrow.array.as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(expected.to_vec()),
    );
    assert_arrays_eq!(
        vortex,
        &PrimitiveArray::from_option_iter(expected).into_array(),
        &mut array_session().create_execution_ctx()
    );
}
