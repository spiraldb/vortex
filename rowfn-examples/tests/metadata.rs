// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, FixedSizeListArray as ArrowList, Float64Array, TimestampNanosecondArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use rowfn_arrow::ArrowOperand;
use rowfn_examples::{AdjustTicks, NoOptions, Scale};
use vortex_array::{IntoArray, VortexSessionExecute, array_session, assert_arrays_eq};
use vortex_array::arrays::{ExtensionArray, FixedSizeListArray, PrimitiveArray};
use vortex_array::dtype::Nullability;
use vortex_array::extension::datetime::{TimeUnit, Timestamp};
use vortex_array::validity::Validity;

use common::{TestResult, pair, run};

#[test]
fn timestamp_metadata_survives_empty_all_null_and_partial_batches() -> TestResult {
    for valid in [vec![], vec![false, false], vec![false, true], vec![true, true]] {
        let rows = valid.len();
        let ticks = vec![10i64; rows];
        let array: ArrayRef = Arc::new(TimestampNanosecondArray::new(ticks.clone().into(), Some(NullBuffer::from(valid.clone()))).with_timezone("America/New_York"));
        let mut field = Field::new("event", array.data_type().clone(), true);
        field.metadata_mut().insert("source".into(), "rowfn-proof".into());
        let arrow = ArrowOperand { column: array, dtype: field.clone(), scalar: false };
        let storage = PrimitiveArray::new(ticks, Validity::from_iter(valid.iter().copied())).into_array();
        let dtype = Timestamp::new_with_tz(TimeUnit::Nanoseconds, Some(Arc::from("America/New_York")), Nullability::Nullable).erased();
        let vortex = ExtensionArray::try_new(dtype.clone(), storage)?.into_array();
        let (output, vortex) = run(&AdjustTicks, vec![(arrow, vortex), pair(&[2], &[true], true, rows)], rows)?;
        assert_eq!(output.field, field.with_name("result"));
        let values = output.array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), valid.iter().map(|valid| valid.then_some(12)).collect::<Vec<_>>());
        assert_eq!(vortex.dtype().as_extension_opt(), Some(&dtype));
        let expected = ExtensionArray::try_new(dtype, PrimitiveArray::from_option_iter(
            valid.iter().map(|valid| valid.then_some(12i64))).into_array())?.into_array();
        assert_arrays_eq!(&vortex, &expected, &mut array_session().create_execution_ctx());
    }
    Ok(())
}

#[test]
fn list_scaling_preserves_shape_and_prepares_each_constant_factor() -> TestResult {
    for width in [0, 1, 2, 3, 4, 7] {
        for rows in [0, 3] {
            for factor in [2.0, -3.0] {
                let values: Vec<f64> = (0..width * rows).map(|i| i as f64).collect();
                let valid: Vec<bool> = (0..rows).map(|i| i != 1).collect();
                let child = Arc::new(Field::new("item", DataType::Float64, false));
                let array: ArrayRef = Arc::new(ArrowList::try_new_with_length(child, width as i32,
                    Arc::new(Float64Array::from(values.clone())), Some(NullBuffer::from(valid.clone())), rows)?);
                let input = ArrowOperand { dtype: Field::new("input", array.data_type().clone(), true), column: array, scalar: false };
                let vortex = FixedSizeListArray::try_new(PrimitiveArray::from_iter(values.clone()).into_array(), width as u32,
                    Validity::from_iter(valid.iter().copied()), rows)?.into_array();
                let factor_array: ArrayRef = Arc::new(Float64Array::from(vec![factor]));
                let factor_arrow = ArrowOperand { dtype: Field::new("factor", DataType::Float64, false), column: factor_array, scalar: true };
                let factor_vortex = vortex_array::arrays::ConstantArray::new(factor, rows).into_array();
                let (arrow, vortex) = run(&Scale, vec![(input, vortex), (factor_arrow, factor_vortex)], rows)?;
                let expected = FixedSizeListArray::try_new(PrimitiveArray::from_iter(values.iter().map(|v| v * factor)).into_array(),
                    width as u32, Validity::from_iter(valid), rows)?.into_array();
                assert_arrays_eq!(&vortex, &expected, &mut array_session().create_execution_ctx());
                let list = arrow.array.as_any().downcast_ref::<ArrowList>().unwrap();
                assert_eq!(list.len(), rows);
                assert_eq!(list.value_length(), width as i32);
                assert_eq!(list.values().as_any().downcast_ref::<Float64Array>().unwrap().values().as_ref(),
                    values.iter().map(|value| value * factor).collect::<Vec<_>>());
                assert_eq!(list.null_count(), usize::from(rows != 0));
            }
        }
    }
    Ok(())
}

#[test]
fn list_binding_rejects_nullable_children_and_factor_mismatch() {
    let child = Arc::new(Field::new("item", DataType::Float64, true));
    let list = Field::new("input", DataType::FixedSizeList(child, 2), true);
    assert!(rowfn_arrow::plan(&Scale, &NoOptions, &[list, Field::new("factor", DataType::Float64, false)]).is_err());
    let child = Arc::new(Field::new("item", DataType::Float64, false));
    let list = Field::new("input", DataType::FixedSizeList(child, 2), false);
    assert!(rowfn_arrow::plan(&Scale, &NoOptions, &[list, Field::new("factor", DataType::Float32, false)]).is_err());
    let bad = ArrowList::try_new_with_length(Arc::new(Field::new("item", DataType::Float64, false)), 2,
        Arc::new(Float64Array::from(vec![1.0])), None, 1);
    assert!(bad.is_err());
}

#[test]
fn sliced_float32_lists_share_runtime_dispatch() -> TestResult {
    let children = vec![0f32, 1.0, 2.0, 3.0, 4.0, 5.0];
    let valid = vec![true, false, true];
    let child = Arc::new(Field::new("item", DataType::Float32, false));
    let list: ArrayRef = Arc::new(ArrowList::try_new_with_length(child, 2,
        Arc::new(arrow_array::Float32Array::from(children.clone())), Some(NullBuffer::from(valid.clone())), 3)?);
    let list = list.slice(1, 2);
    let arrow = ArrowOperand { dtype: Field::new("input", list.data_type().clone(), true), column: list, scalar: false };
    let vortex = FixedSizeListArray::try_new(PrimitiveArray::from_iter(children).into_array(), 2,
        Validity::from_iter(valid), 3)?.into_array().slice(1..3)?;
    let factor = ArrowOperand { column: Arc::new(arrow_array::Float32Array::from(vec![2.0])),
        dtype: Field::new("factor", DataType::Float32, false), scalar: true };
    let (arrow, vortex) = run(&Scale, vec![(arrow, vortex), (factor,
        vortex_array::arrays::ConstantArray::new(2f32, 2).into_array())], 2)?;
    let list = arrow.array.as_any().downcast_ref::<ArrowList>().unwrap();
    assert_eq!(list.values().as_any().downcast_ref::<arrow_array::Float32Array>().unwrap().values().as_ref(), &[4f32, 6.0, 8.0, 10.0]);
    assert!(list.is_null(0));
    assert!(!list.is_null(1));
    let expected = FixedSizeListArray::try_new(PrimitiveArray::from_iter([4f32, 6.0, 8.0, 10.0]).into_array(),
        2, Validity::from_iter([false, true]), 2)?.into_array();
    assert_arrays_eq!(&vortex, &expected, &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn timestamp_units_and_null_only_overflow_preserve_ticks() -> TestResult {
    let units = [
        (arrow_schema::TimeUnit::Second, TimeUnit::Seconds),
        (arrow_schema::TimeUnit::Millisecond, TimeUnit::Milliseconds),
        (arrow_schema::TimeUnit::Microsecond, TimeUnit::Microseconds),
        (arrow_schema::TimeUnit::Nanosecond, TimeUnit::Nanoseconds),
    ];
    for (arrow_unit, vortex_unit) in units {
        let dtype = DataType::Timestamp(arrow_unit, None);
        let data = arrow_array::Int64Array::new(vec![i64::MAX, 10].into(), Some(vec![false, true].into()))
            .to_data().into_builder().data_type(dtype.clone()).build()?;
        let arrow = ArrowOperand { column: arrow_array::make_array(data), dtype: Field::new("input", dtype.clone(), true), scalar: false };
        let vortex_dtype = Timestamp::new(vortex_unit, Nullability::Nullable).erased();
        let vortex = ExtensionArray::try_new(vortex_dtype.clone(), PrimitiveArray::new(vec![i64::MAX, 10],
            Validity::from_iter([false, true])).into_array())?.into_array();
        let (arrow, vortex) = run(&AdjustTicks, vec![(arrow, vortex), pair(&[2], &[true], true, 2)], 2)?;
        assert_eq!(arrow.array.data_type(), &dtype);
        assert_eq!(arrow.array.null_count(), 1);
        let expected = ExtensionArray::try_new(vortex_dtype,
            PrimitiveArray::from_option_iter([None, Some(12i64)]).into_array())?.into_array();
        assert_arrays_eq!(&vortex, &expected, &mut array_session().create_execution_ctx());
    }
    Ok(())
}

#[test]
fn output_field_mismatch_is_rejected_even_when_all_rows_are_null() -> TestResult {
    let (input, _) = pair(&[0, 0], &[false, false], false, 2);
    let output = rowfn_arrow::plan(&rowfn_examples::Add::<true>, &NoOptions, &[input.dtype.clone(), input.dtype.clone()])?;
    let wrong = output.with_nullable(false);
    assert!(rowfn_arrow::invoke(&rowfn_examples::Add::<true>, &NoOptions, &[input.clone(), input], 2, wrong).is_err());
    Ok(())
}
