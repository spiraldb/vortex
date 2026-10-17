// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::BooleanArray;
use arrow_array::Int64Array;
use arrow_array::LargeStringArray;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_schema::DataType;
use arrow_schema::Field;
use rowfn_arrow::ArrowOperand;
use rowfn_examples::Concat;
use rowfn_examples::Multiply;
use rowfn_examples::StringPredicate;
use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::array_session;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::assert_arrays_eq;

use common::TestResult;
use common::errors;
use common::pair;
use common::run;

fn strings(values: &[Option<&str>], scalar: bool, rows: usize) -> (ArrowOperand, ArrayRef) {
    let column = Arc::new(StringViewArray::from(values.to_vec()));
    let vortex = if scalar {
        ConstantArray::new(values[0], rows).into_array()
    } else {
        VarBinViewArray::from_iter_nullable_str(values.iter().copied()).into_array()
    };
    let input = ArrowOperand {
        column,
        dtype: Field::new("input", DataType::Utf8View, true),
        scalar,
    };

    (input, vortex)
}

#[rstest]
#[case::prefix(StringPredicate::StartsWith, "a", [true, false, false, false])]
#[case::suffix(StringPredicate::EndsWith, "tail", [true, false, true, false])]
#[case::contains(StringPredicate::Contains, "λ", [false, false, true, false])]
#[case::ascii_case(StringPredicate::EqIgnoreAsciiCase, "ARROW TAIL", [true, true, false, false])]
fn string_predicates_on_both_hosts(
    #[case] function: StringPredicate,
    #[case] pattern: &str,
    #[case] expected: [bool; 4],
) -> TestResult {
    let values = [Some("arrow tail"), Some("ARROW TAIL"), Some("λ tail"), Some(""), None];
    let (arrow, vortex) = run(&function, vec![strings(&values, false, 5), strings(&[Some(pattern)], true, 5)], 5)?;
    let expected: Vec<_> = expected.into_iter().map(Some).chain([None]).collect();
    assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap(), &BooleanArray::from(expected.clone()));
    assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected).into_array(), &mut array_session().create_execution_ctx());

    Ok(())
}

#[test]
fn contains_reprepares_constants_and_preserves_null_patterns() -> TestResult {
    for (pattern, expected) in [(Some(""), Some(true)), (Some("λ"), Some(false)), (None, None)] {
        let (arrow, vortex) = run(
            &StringPredicate::Contains,
            vec![strings(&[Some("arrow")], false, 1), strings(&[pattern], true, 1)],
            1,
        )?;
        assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap(), &BooleanArray::from(vec![expected]));
        assert_arrays_eq!(&vortex, &BoolArray::from_iter([expected]).into_array(), &mut array_session().create_execution_ctx());
    }

    Ok(())
}

#[test]
fn concat_owns_strings_and_combines_both_validities() -> TestResult {
    let left = [Some("λ"), None, Some("arrow"), Some("a long string stored outside its view")];
    let right = [Some(" tail"), Some("ignored"), None, Some(" suffix")];
    let (arrow, vortex) = run(&Concat, vec![strings(&left, false, 4), strings(&right, false, 4)], 4)?;
    let expected = [Some("λ tail"), None, None, Some("a long string stored outside its view suffix")];
    assert_eq!(arrow.array.as_any().downcast_ref::<StringViewArray>().unwrap(), &StringViewArray::from(expected.to_vec()));
    assert_arrays_eq!(&vortex, &VarBinViewArray::from_iter_nullable_str(expected).into_array(), &mut array_session().create_execution_ctx());

    Ok(())
}

#[test]
fn multiply_only_suppresses_null_row_overflow() -> TestResult {
    let (arrow, vortex) = run(
        &Multiply,
        vec![pair(&[i64::MAX, -7], &[false, true], false, 2), pair(&[3], &[true], true, 2)],
        2,
    )?;
    assert_eq!(arrow.array.as_any().downcast_ref::<Int64Array>().unwrap(), &Int64Array::from(vec![None, Some(-21)]));
    assert_arrays_eq!(&vortex, &PrimitiveArray::from_option_iter([None, Some(-21i64)]).into_array(), &mut array_session().create_execution_ctx());

    let (arrow, vortex) = errors(
        &Multiply,
        vec![pair(&[i64::MAX], &[true], false, 1), pair(&[3], &[true], true, 1)],
        1,
    )?;
    assert!(arrow.contains("integer overflow in checked multiply"));
    assert!(vortex.contains("integer overflow in checked multiply"));

    Ok(())
}

#[test]
fn text_predicates_dispatch_mixed_layouts_and_preserve_byte_boundaries() -> TestResult {
    let texts = [Some("skip"), Some("λ short"), Some("a long string ending in λ"), Some(""), None];
    let patterns = [Some("skip"), Some("λ"), Some("λ"), Some(""), Some("x")];
    for left_layout in 0..3 {
        for right_layout in 0..3 {
            for predicate in [StringPredicate::StartsWith, StringPredicate::EndsWith] {
                let operands = [(texts.as_slice(), left_layout), (patterns.as_slice(), right_layout)]
                    .map(|(values, layout)| {
                        let (mut arrow, vortex) = strings(values, false, values.len());
                        arrow.column = match layout {
                            0 => Arc::new(StringArray::from(values.to_vec())),
                            1 => Arc::new(LargeStringArray::from(values.to_vec())),
                            _ => Arc::new(StringViewArray::from(values.to_vec())),
                        };
                        arrow.column = arrow.column.slice(1, values.len() - 1);
                        arrow.dtype = Field::new("input", arrow.column.data_type().clone(), true);
                        (arrow, vortex.slice(1..values.len()).unwrap())
                    });
                let (arrow, vortex) = run(&predicate, Vec::from(operands), texts.len() - 1)?;
                let expected: Vec<_> = texts[1..].iter().zip(&patterns[1..]).map(|(text, pattern)| {
                    text.zip(*pattern).map(|(text, pattern)| match predicate {
                        StringPredicate::StartsWith => text.starts_with(pattern),
                        _ => text.ends_with(pattern),
                    })
                }).collect();
                arrow.array.to_data().validate_full()?;
                assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap(),
                    &BooleanArray::from(expected.clone()));
                assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected).into_array(),
                    &mut array_session().create_execution_ctx());
            }
        }
    }
    Ok(())
}
