// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, DictionaryArray, Int8Array, Int64Array, LargeStringArray, StringArray, StringViewArray};
use arrow_array::types::Int8Type;
use arrow_schema::{DataType, Field};
use rowfn_arrow::ArrowOperand;
use rowfn_examples::{Add, NoOptions, Not, Trim};
use rstest::rstest;
use vortex_array::{IntoArray, VortexSessionExecute, array_session, assert_arrays_eq};
use vortex_array::arrays::{BoolArray, VarBinViewArray};

use common::{TestResult, pair, run};

fn invoke<F: rowfn::RowFn<rowfn_arrow::ArrowHost, Options = NoOptions>>(function: &F, column: ArrayRef) -> TestResult<rowfn_arrow::Output> {
    let rows = column.len();
    let input = ArrowOperand { dtype: Field::new("input", column.data_type().clone(), true), column, scalar: false };
    let output = rowfn_arrow::plan(function, &NoOptions, &[input.dtype.clone()])?;
    Ok(rowfn_arrow::invoke(function, &NoOptions, &[input], rows, output)?)
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(7)]
#[case(63)]
#[case(64)]
#[case(65)]
#[case(127)]
#[case(128)]
#[case(129)]
fn boolean_slices_preserve_offsets_and_tails(#[case] len: usize) -> TestResult {
    for offset in [1, 3, 7, 11] {
        let values: Vec<_> = (0..offset + len + 1).map(|i| if i % 5 == 0 { None } else { Some(i % 3 == 0) }).collect();
        let arrow: ArrayRef = Arc::new(BooleanArray::from(values.clone()).slice(offset, len));
        let vortex = BoolArray::from_iter(values.clone()).into_array().slice(offset..offset + len)?;
        let input = ArrowOperand { dtype: Field::new("input", DataType::Boolean, true), column: arrow, scalar: false };
        let (arrow, vortex) = run(&Not, vec![(input, vortex)], len)?;
        let expected: Vec<_> = values[offset..offset + len].iter().map(|v| v.map(|v| !v)).collect();
        assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap().iter().collect::<Vec<_>>(), expected);
        assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected).into_array(), &mut array_session().create_execution_ctx());
    }
    Ok(())
}

#[test]
fn dictionary_null_values_and_unused_overflow() -> TestResult {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(4), None, Some(i64::MAX)]));
    let keys = Int8Array::from(vec![Some(0), Some(1), None, Some(0)]);
    let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values)?);
    let input = ArrowOperand { dtype: Field::new("input", dictionary.data_type().clone(), true), column: dictionary, scalar: false };
    let rhs = pair(&[1], &[true], true, 4).0;
    let output = rowfn_arrow::plan(&Add::<true>, &NoOptions, &[input.dtype.clone(), rhs.dtype.clone()])?;
    let result = rowfn_arrow::invoke(&Add::<true>, &NoOptions, &[input, rhs], 4, output)?;
    assert_eq!(result.array.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>(), vec![Some(5), None, None, Some(5)]);
    Ok(())
}

#[test]
fn string_layouts_slices_and_output_ownership() -> TestResult {
    let values = [Some("ignore"), Some("  short  "), None, Some("  a longer string stored outside the inline view  "), Some("tail")];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(values.to_vec())),
        Arc::new(LargeStringArray::from(values.to_vec())),
        Arc::new(StringViewArray::from(values.to_vec())),
    ];
    for input in arrays {
        let input = input.slice(1, 3);
        let output = invoke(&Trim, input)?;
        // The input owners were consumed by invoke and dropped before result access.
        let result = output.array.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(result.iter().collect::<Vec<_>>(), vec![Some("short"), None, Some("a longer string stored outside the inline view")]);
        assert_eq!(output.field.data_type(), &DataType::Utf8View);
    }
    let arrow: ArrayRef = Arc::new(StringArray::from(values.to_vec()).slice(1, 3));
    let vortex = VarBinViewArray::from_iter_nullable_str(values).into_array().slice(1..4)?;
    let input = ArrowOperand { dtype: Field::new("input", DataType::Utf8, true), column: arrow, scalar: false };
    let (_, result) = run(&Trim, vec![(input, vortex)], 3)?;
    let expected = VarBinViewArray::from_iter_nullable_str([Some("short"), None, Some("a longer string stored outside the inline view")]).into_array();
    assert_arrays_eq!(&result, &expected, &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn dictionary_strings_preserve_value_nulls() -> TestResult {
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some(" x "), None, Some(" unused ")]));
    let dictionary = DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(0), Some(1), None]), values)?;
    let output = invoke(&Trim, Arc::new(dictionary))?;
    assert_eq!(output.array.as_any().downcast_ref::<StringViewArray>().unwrap().iter().collect::<Vec<_>>(), vec![Some("x"), None, None]);
    Ok(())
}

#[test]
fn unknown_extensions_do_not_become_integers() {
    let mut field = Field::new("input", DataType::Int64, false);
    field.metadata_mut().insert("ARROW:extension:name".into(), "foreign.money".into());
    assert!(rowfn_arrow::plan(&Add::<false>, &NoOptions, &[field.clone(), field]).is_err());
}

#[test]
fn masked_vortex_string_constant_retains_value_after_null_first_row() -> TestResult {
    let column = vortex_array::arrays::MaskedArray::try_new(
        vortex_array::arrays::ConstantArray::new("  retained constant  ", 3).into_array(),
        vortex_array::validity::Validity::from_iter([false, true, true]),
    )?.into_array();
    let arrow: ArrayRef = Arc::new(StringArray::from(vec![None, Some("  retained constant  "), Some("  retained constant  ")]));
    let input = ArrowOperand { dtype: Field::new("input", DataType::Utf8, true), column: arrow, scalar: false };
    let (arrow, vortex) = run(&Trim, vec![(input, column)], 3)?;
    let expected = [None, Some("retained constant"), Some("retained constant")];
    assert_eq!(arrow.array.as_any().downcast_ref::<StringViewArray>().unwrap().iter().collect::<Vec<_>>(), expected);
    assert_arrays_eq!(&vortex, &VarBinViewArray::from_iter_nullable_str(expected).into_array(),
        &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn empty_and_null_strings_keep_planned_view_metadata() -> TestResult {
    for values in [Vec::<Option<&str>>::new(), vec![None, None]] {
        let result = invoke(&Trim, Arc::new(LargeStringArray::from(values)))?;
        assert_eq!(result.array.data_type(), &DataType::Utf8View);
        assert_eq!(result.field, Field::new("result", DataType::Utf8View, true));
        assert_eq!(result.array.len(), result.array.null_count());
    }
    Ok(())
}
