// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::types::Int8Type;
use arrow_array::{Array, BooleanArray, DictionaryArray, Int8Array, StringViewArray};
use arrow_schema::{DataType, Field};
use rowfn_arrow::ArrowOperand;
use rowfn_examples::{Like, RegexpIsMatch};
use vortex_array::arrays::{BoolArray, ConstantArray, VarBinViewArray};
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute, array_session, assert_arrays_eq};

use common::{TestResult, errors, run};

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

fn check_pattern<F: rowfn::RowFn<rowfn_arrow::ArrowHost, Options = rowfn_examples::NoOptions>
    + rowfn::RowFn<vortex_array::scalar_fn::unstable::rowfn::VortexHost, Options = rowfn_examples::NoOptions>>(
    function: &F,
    texts: &[Option<&str>],
    patterns: &[Option<&str>],
    expected: &[Option<bool>],
) -> TestResult {
    let (arrow, vortex) = run(function, vec![
        strings(texts, false, texts.len()),
        strings(patterns, false, texts.len()),
    ], texts.len())?;

    assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap(),
        &BooleanArray::from(expected.to_vec()));
    assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected.iter().copied()).into_array(),
        &mut array_session().create_execution_ctx());

    Ok(())
}

#[test]
fn like_wildcards_escapes_negation_and_nulls() -> TestResult {
    let texts = [
        Some("AλB"),
        Some("a%b"),
        Some("head\ntail"),
        Some("plain"),
        None,
        Some("ignored"),
    ];
    let patterns = [
        Some("A_B"),
        Some(r"a\%b"),
        Some("%tail"),
        Some("p%"),
        Some("%"),
        None,
    ];

    check_pattern(&Like::<false, false>, &texts, &patterns,
        &[Some(true), Some(true), Some(true), Some(true), None, None])?;
    check_pattern(&Like::<false, true>, &texts, &patterns,
        &[Some(false), Some(false), Some(false), Some(false), None, None])?;
    check_pattern(&Like::<true, false>, &texts, &patterns,
        &[Some(true), Some(true), Some(true), Some(true), None, None])?;
    check_pattern(&Like::<true, true>, &texts, &patterns,
        &[Some(false), Some(false), Some(false), Some(false), None, None])?;

    Ok(())
}

#[test]
fn like_variants_match_arrow_for_varying_patterns() -> TestResult {
    let texts = [Some("a%b"), Some("AλB"), Some("Kite"), Some("head\ntail"), None];
    let patterns = [Some(r"a\%b"), Some("a_b"), Some("k%"), Some("%tail"), Some("(")];
    let left = StringViewArray::from(texts.to_vec());
    let right = StringViewArray::from(patterns.to_vec());

    let expected = arrow_string::like::like(&left, &right)?;
    check_pattern(&Like::<false, false>, &texts, &patterns,
        &expected.iter().collect::<Vec<_>>())?;

    let expected = arrow_string::like::nlike(&left, &right)?;
    check_pattern(&Like::<false, true>, &texts, &patterns,
        &expected.iter().collect::<Vec<_>>())?;

    let expected = arrow_string::like::ilike(&left, &right)?;
    check_pattern(&Like::<true, false>, &texts, &patterns,
        &expected.iter().collect::<Vec<_>>())?;

    let expected = arrow_string::like::nilike(&left, &right)?;
    check_pattern(&Like::<true, true>, &texts, &patterns,
        &expected.iter().collect::<Vec<_>>())?;

    Ok(())
}

#[test]
fn repeated_and_alternating_patterns_match_arrow() -> TestResult {
    let texts: Vec<_> = (0..128).map(|index| {
        if index % 13 == 0 { None }
        else if index % 2 == 0 { Some("alpha end") }
        else { Some("beta end") }
    }).collect();
    let patterns: Vec<_> = (0..128).map(|index| {
        if index % 2 == 0 { Some("^alpha.*end$") }
        else { Some("^beta.*end$") }
    }).collect();
    let baseline = arrow_string::regexp::regexp_is_match(
        &StringViewArray::from(texts.clone()),
        &StringViewArray::from(patterns.clone()),
        None::<&StringViewArray>,
    )?;
    check_pattern(&RegexpIsMatch, &texts, &patterns,
        &baseline.iter().collect::<Vec<_>>())?;

    let patterns: Vec<_> = (0..128).map(|index| {
        if index % 2 == 0 { Some("a%end") }
        else { Some("b%end") }
    }).collect();
    let baseline = arrow_string::like::like(
        &StringViewArray::from(texts.clone()),
        &StringViewArray::from(patterns.clone()),
    )?;
    check_pattern(&Like::<false, false>, &texts, &patterns,
        &baseline.iter().collect::<Vec<_>>())
}

#[test]
fn dictionary_value_nulls_and_unused_invalid_patterns_do_not_compile() -> TestResult {
    let texts = [Some("alpha"), Some("ignored"), Some("another"), Some("ignored")];
    let (text_operand, vortex_text) = strings(&texts, false, texts.len());

    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), Some(0), None]),
        Arc::new(StringViewArray::from(vec![Some("a%"), None, Some("[")])),
    )?;
    let pattern_operand = ArrowOperand {
        dtype: Field::new("pattern", dictionary.data_type().clone(), true),
        column: Arc::new(dictionary.clone()),
        scalar: false,
    };
    let vortex_pattern = VarBinViewArray::from_iter_nullable_str([
        Some("a%"), None, Some("a%"), None,
    ]).into_array();

    let baseline = arrow_string::like::like(
        &StringViewArray::from(texts.to_vec()), &dictionary,
    )?;
    let (arrow, vortex) = run(&Like::<false, false>, vec![
        (text_operand, vortex_text),
        (pattern_operand, vortex_pattern),
    ], texts.len())?;
    let expected = BooleanArray::from(vec![Some(true), None, Some(true), None]);
    assert_eq!(baseline, expected);
    assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap(), &expected);
    assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected.iter()).into_array(),
        &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn ilike_follows_unicode_case_folding() -> TestResult {
    let texts = [Some("Kite"), Some("Kite"), Some("Straße"), Some("éclair"), Some("\n"), Some("")];
    let patterns = [Some("k%"), Some("k%"), Some("STRASSE"), Some("É%"), Some("_"), Some("%")];
    check_pattern(&Like::<true, false>, &texts, &patterns,
        &[Some(true), Some(true), Some(false), Some(true), Some(true), Some(true)])
}

#[test]
fn regexp_matches_only_valid_rows() -> TestResult {
    let texts = [Some("Foo"), Some("Bar"), None, Some(""), Some("FooBar")];
    let patterns = [Some("^F"), Some("Bar$"), Some("("), Some(""), None];
    check_pattern(&RegexpIsMatch, &texts, &patterns,
        &[Some(true), Some(true), None, Some(true), None])?;

    let expected = arrow_string::regexp::regexp_is_match(
        &StringViewArray::from(texts.to_vec()),
        &StringViewArray::from(patterns.to_vec()),
        None::<&StringViewArray>,
    )?;
    assert_eq!(expected.iter().collect::<Vec<_>>(),
        vec![Some(true), Some(true), None, Some(true), None]);
    Ok(())
}

#[test]
fn invalid_regex_on_valid_row_is_an_error() -> TestResult {
    let (arrow, vortex) = errors(&RegexpIsMatch, vec![
        strings(&[Some("value")], false, 1),
        strings(&[Some("(")], false, 1),
    ], 1)?;
    assert!(arrow.contains("regular expression did not compile"));
    assert!(vortex.contains("regular expression did not compile"));
    Ok(())
}

#[test]
fn scalar_pattern_is_prepared_for_each_invocation() -> TestResult {
    for (pattern, expected) in [("foo%", Some(true)), ("bar%", Some(false))] {
        let (arrow, vortex) = run(&Like::<false, false>, vec![
            strings(&[Some("foobar")], false, 1),
            strings(&[Some(pattern)], true, 1),
        ], 1)?;
        assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![expected]));
        assert_arrays_eq!(&vortex, &BoolArray::from_iter([expected]).into_array(),
            &mut array_session().create_execution_ctx());

        let baseline = arrow_string::like::like(
            &StringViewArray::from(vec![Some("foobar")]),
            &StringViewArray::new_scalar(pattern),
        )?;
        assert_eq!(baseline, BooleanArray::from(vec![expected]));
    }

    let texts = [Some("Foo"), None, Some("Bar")];
    let expected = arrow_string::regexp::regexp_is_match_scalar(
        &StringViewArray::from(texts.to_vec()), "^F", None)?;
    let (arrow, vortex) = run(&RegexpIsMatch, vec![
        strings(&texts, false, 3),
        strings(&[Some("^F")], true, 3),
    ], 3)?;
    assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap(), &expected);
    assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected.iter()).into_array(),
        &mut array_session().create_execution_ctx());

    Ok(())
}

#[test]
fn scalar_only_inputs_broadcast_after_one_prepared_row() -> TestResult {
    let (arrow, vortex) = run(&Like::<false, false>, vec![
        strings(&[Some("foobar")], true, 9),
        strings(&[Some("foo%")], true, 9),
    ], 9)?;
    let expected = BooleanArray::from(vec![Some(true); 9]);
    assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap(), &expected);
    assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected.iter()).into_array(),
        &mut array_session().create_execution_ctx());

    let (arrow, vortex) = run(&RegexpIsMatch, vec![
        strings(&[Some("foobar")], true, 9),
        strings(&[Some("^foo")], true, 9),
    ], 9)?;
    assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap(), &expected);
    assert_arrays_eq!(&vortex, &BoolArray::from_iter(expected.iter()).into_array(),
        &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn invalid_regex_only_in_null_rows_is_not_an_error() -> TestResult {
    let texts = [None, Some("valid")];
    let patterns = [Some("("), Some("^v")];
    let expected = arrow_string::regexp::regexp_is_match(
        &StringViewArray::from(texts.to_vec()),
        &StringViewArray::from(patterns.to_vec()),
        None::<&StringViewArray>,
    )?;
    check_pattern(&RegexpIsMatch, &texts, &patterns,
        &expected.iter().collect::<Vec<_>>())
}

#[test]
fn invalid_scalar_regex_on_all_null_rows_follows_strict_contract() -> TestResult {
    let (arrow, vortex) = run(&RegexpIsMatch, vec![
        strings(&[None, None], false, 2),
        strings(&[Some("(")], true, 2),
    ], 2)?;
    let expected = BooleanArray::from(vec![None, None]);
    assert_eq!(arrow.array.as_any().downcast_ref::<BooleanArray>().unwrap(), &expected);
    assert_arrays_eq!(&vortex, &BoolArray::from_iter([None, None]).into_array(),
        &mut array_session().create_execution_ctx());

    // Arrow compiles its scalar pattern before it sees that every input row is null.
    assert!(arrow_string::regexp::regexp_is_match_scalar(
        &StringViewArray::from(vec![None::<&str>, None]), "(", None,
    ).is_err());
    Ok(())
}

#[test]
fn empty_and_all_null_batches_keep_boolean_type() -> TestResult {
    for texts in [Vec::new(), vec![None, None]] {
        let patterns = vec![Some("%") ; texts.len()];
        let expected = vec![None; texts.len()];
        check_pattern(&Like::<false, false>, &texts, &patterns, &expected)?;
    }
    Ok(())
}
