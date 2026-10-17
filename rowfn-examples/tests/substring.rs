// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::LargeStringArray;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_buffer::Buffer;
use arrow_buffer::NullBuffer;
use arrow_buffer::OffsetBuffer;
use arrow_schema::Field;
use rowfn_arrow::ArrowOperand;
use rowfn_examples::NoOptions;
use rowfn_examples::SubstringBytes;
use rowfn_examples::SubstringChars;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::array_session;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::assert_arrays_eq;
use vortex_array::scalar_fn::unstable::rowfn::VortexHost;

use common::TestResult;
use common::errors;
use common::run;

fn strings(values: &[Option<&str>], layout: usize) -> (ArrowOperand, ArrayRef) {
    let column: ArrowArrayRef = match layout {
        0 => Arc::new(StringArray::from(values.to_vec())),
        1 => Arc::new(LargeStringArray::from(values.to_vec())),
        _ => Arc::new(StringViewArray::from(values.to_vec())),
    };
    let dtype = Field::new("text", column.data_type().clone(), true);
    let vortex = VarBinViewArray::from_iter_nullable_str(values.iter().copied()).into_array();
    (ArrowOperand { column, dtype, scalar: false }, vortex)
}

#[test]
fn substring_matches_arrow_for_supported_byte_bounds() -> TestResult {
    let values = [Some("arrow"), Some("éclair"), None, Some("a longer string outside a view")];
    let cases = [
        (SubstringBytes::new(0, Some(2)), [Some("ar"), Some("é"), None, Some("a ")]),
        (SubstringBytes::new(-1, None), [Some("w"), Some("r"), None, Some("w")]),
        (SubstringBytes::new(40, None), [Some(""), Some(""), None, Some("")]),
        (SubstringBytes::new(-2, Some(2)), [Some("ow"), Some("ir"), None, Some("ew")]),
    ];

    for layout in 0..3 {
        for (function, expected) in cases {
            let (arrow_operand, vortex_operand) = strings(&values, layout);
            if layout != 2 {
                let baseline = arrow_string::substring::substring(
                    arrow_operand.column.as_ref(), function.start, function.length,
                )?;
                let actual: Vec<_> = match layout {
                    0 => baseline.as_any().downcast_ref::<StringArray>().unwrap().iter().collect(),
                    _ => baseline.as_any().downcast_ref::<LargeStringArray>().unwrap().iter().collect(),
                };
                assert_eq!(actual, expected);
            }

            let (arrow, vortex) = run(&function, vec![(arrow_operand, vortex_operand)], values.len())?;
            arrow.array.to_data().validate_full()?;
            assert_eq!(arrow.array.as_any().downcast_ref::<StringViewArray>().unwrap(),
                &StringViewArray::from(expected.to_vec()));
            assert_arrays_eq!(&vortex, &VarBinViewArray::from_iter_nullable_str(expected).into_array(),
                &mut array_session().create_execution_ctx());
        }
    }

    Ok(())
}

#[test]
fn substring_reports_invalid_boundary_on_valid_rows() -> TestResult {
    let function = SubstringBytes::new(1, None);
    let (arrow, vortex) = errors(&function, vec![strings(&[Some("é")], 0)], 1)?;
    assert!(arrow.contains("invalid utf-8 boundary"));
    assert!(vortex.contains("invalid utf-8 boundary"));

    let function = SubstringBytes::new(0, Some(1));
    let (arrow, vortex) = errors(&function, vec![strings(&[Some("é")], 1)], 1)?;
    assert!(arrow.contains("invalid utf-8 boundary"));
    assert!(vortex.contains("invalid utf-8 boundary"));

    let function = SubstringBytes::new(-6, None);
    let (arrow, vortex) = errors(&function, vec![strings(&[Some("éclair")], 2)], 1)?;
    assert!(arrow.contains("invalid utf-8 boundary"));
    assert!(vortex.contains("invalid utf-8 boundary"));
    Ok(())
}

#[test]
fn substring_ignores_invalid_boundaries_in_null_payloads() -> TestResult {
    let column: ArrowArrayRef = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0, 2, 4].into()),
        Buffer::from("éok".as_bytes()),
        Some(NullBuffer::from(vec![false, true])),
    ));
    let operand = ArrowOperand {
        dtype: Field::new("text", column.data_type().clone(), true),
        column,
        scalar: false,
    };
    let function = SubstringBytes::new(1, None);
    assert!(arrow_string::substring::substring(operand.column.as_ref(), 1, None).is_err());

    let vortex = VarBinViewArray::from_iter_nullable_str([None, Some("ok")]).into_array();
    let (arrow, vortex) = run(&function, vec![(operand, vortex)], 2)?;
    let expected = [None, Some("k")];
    assert_eq!(arrow.array.as_any().downcast_ref::<StringViewArray>().unwrap(),
        &StringViewArray::from(expected.to_vec()));
    assert_arrays_eq!(&vortex, &VarBinViewArray::from_iter_nullable_str(expected).into_array(),
        &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn substring_rejects_bounds_that_arrow_would_narrow() -> TestResult {
    let (arrow, vortex) = strings(&[Some("abc")], 0);
    for function in [
        SubstringBytes::new(i64::MAX, None),
        SubstringBytes::new(0, Some(u64::MAX)),
    ] {
        let arrow_error = rowfn_arrow::plan(&function, &NoOptions, &[arrow.dtype.clone()])
            .err().ok_or("expected Arrow planning error")?;
        assert!(arrow_error.to_string().contains("signed 32-bit offset"));

        let vortex_error = rowfn::plan::<VortexHost, _>(&function, &NoOptions, &[vortex.dtype().clone()])
            .err().ok_or("expected Vortex planning error")?;
        assert!(vortex_error.to_string().contains("signed 32-bit offset"));
    }
    Ok(())
}

#[test]
fn substring_by_char_matches_arrow_on_ascii_and_unicode() -> TestResult {
    let values = [Some("arrow"), Some("éclair"), None, Some("a longer string outside a view")];
    let cases = [
        (SubstringChars::new(1, Some(2)), [Some("rr"), Some("cl"), None, Some(" l")]),
        (SubstringChars::new(-3, Some(2)), [Some("ro"), Some("ai"), None, Some("ie")]),
        (SubstringChars::new(50, None), [Some(""), Some(""), None, Some("")]),
        (SubstringChars::new(i64::MIN, None), [Some("arrow"), Some("éclair"), None,
            Some("a longer string outside a view")]),
    ];

    for layout in 0..3 {
        for (function, expected) in cases {
            let (arrow_operand, vortex_operand) = strings(&values, layout);
            if layout != 2 {
                let actual: Vec<Option<String>> = match layout {
                    0 => {
                        let baseline = arrow_string::substring::substring_by_char(
                            arrow_operand.column.as_any().downcast_ref::<StringArray>().unwrap(),
                            function.start,
                            function.length,
                        )?;
                        baseline.iter().map(|value| value.map(str::to_owned)).collect()
                    }
                    _ => {
                        let baseline = arrow_string::substring::substring_by_char(
                            arrow_operand.column.as_any().downcast_ref::<LargeStringArray>().unwrap(),
                            function.start,
                            function.length,
                        )?;
                        baseline.iter().map(|value| value.map(str::to_owned)).collect()
                    }
                };
                assert_eq!(actual.iter().map(|value| value.as_deref()).collect::<Vec<_>>(), expected);
            }

            let (arrow, vortex) = run(&function, vec![(arrow_operand, vortex_operand)], values.len())?;
            assert_eq!(arrow.array.as_any().downcast_ref::<StringViewArray>().unwrap(),
                &StringViewArray::from(expected.to_vec()));
            assert_arrays_eq!(&vortex, &VarBinViewArray::from_iter_nullable_str(expected).into_array(),
                &mut array_session().create_execution_ctx());
        }
    }
    Ok(())
}

#[test]
fn substring_by_char_keeps_sliced_output_after_inputs_are_dropped() -> TestResult {
    let values = [Some("discard"), Some("héllo"), Some("a long input stored outside a view"), None];
    let (mut arrow, vortex) = strings(&values, 2);
    arrow.column = arrow.column.slice(1, 3);
    let vortex = vortex.slice(1..4)?;
    let (arrow, vortex) = run(&SubstringChars::new(1, Some(2)), vec![(arrow, vortex)], 3)?;
    let expected = [Some("él"), Some(" l"), None];
    assert_eq!(arrow.array.as_any().downcast_ref::<StringViewArray>().unwrap(),
        &StringViewArray::from(expected.to_vec()));
    assert_arrays_eq!(&vortex, &VarBinViewArray::from_iter_nullable_str(expected).into_array(),
        &mut array_session().create_execution_ctx());
    Ok(())
}
