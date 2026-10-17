// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::Int32Array;
use arrow_array::Int64Array;
use arrow_array::LargeStringArray;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_schema::Field;
use rowfn::RowFn;
use rowfn_arrow::ArrowHost;
use rowfn_arrow::ArrowOperand;
use rowfn_examples::BitLength;
use rowfn_examples::ByteLength;
use rowfn_examples::NoOptions;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::array_session;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::assert_arrays_eq;
use vortex_array::scalar_fn::unstable::rowfn::VortexHost;

use common::TestResult;
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

fn check<F>(function: &F, values: &[Option<&str>], layout: usize, bits: bool) -> TestResult
where
    F: RowFn<ArrowHost, Options = NoOptions> + RowFn<VortexHost, Options = NoOptions>,
{
    let (arrow_operand, vortex_operand) = strings(values, layout);
    let baseline = if bits {
        arrow_string::length::bit_length(arrow_operand.column.as_ref())?
    } else {
        arrow_string::length::length(arrow_operand.column.as_ref())?
    };
    let expected: Vec<_> = values.iter().map(|value| value.map(|text| {
        let length = text.len() as i32;
        if bits { length.wrapping_mul(8) } else { length }
    })).collect();
    let (arrow, vortex) = run(function, vec![(arrow_operand, vortex_operand)], values.len())?;

    if layout == 1 {
        let expected_large: Vec<_> = expected.iter().map(|value| value.map(i64::from)).collect();
        assert_eq!(baseline.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(expected_large.clone()));
        assert_eq!(arrow.array.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(expected_large));
    } else {
        assert_eq!(baseline.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(expected.clone()));
        assert_eq!(arrow.array.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(expected.clone()));
    }
    assert_arrays_eq!(&vortex, &PrimitiveArray::from_option_iter(expected).into_array(),
        &mut array_session().create_execution_ctx());
    Ok(())
}

#[test]
fn byte_and_bit_lengths_match_arrow_on_all_string_layouts() -> TestResult {
    let values = [Some(""), Some("é"), None, Some("a long string stored outside its view")];
    for layout in 0..3 {
        check(&ByteLength, &values, layout, false)?;
        check(&BitLength, &values, layout, true)?;
        check(&ByteLength, &[None, None], layout, false)?;
        check(&BitLength, &[], layout, true)?;
    }
    Ok(())
}

#[test]
fn byte_and_bit_lengths_preserve_sliced_validity() -> TestResult {
    let values = [Some("discard"), Some("é"), None, Some("a long string")];
    for layout in 0..3 {
        let (mut arrow, vortex) = strings(&values, layout);
        arrow.column = arrow.column.slice(1, 3);
        let vortex = vortex.slice(1..4)?;
        let expected = [Some(2), None, Some(13)];
        for (bits, expected) in [(false, expected), (true, [Some(16), None, Some(104)])] {
            let (arrow_result, vortex_result) = if bits {
                run(&BitLength, vec![(arrow.clone(), vortex.clone())], 3)?
            } else {
                run(&ByteLength, vec![(arrow.clone(), vortex.clone())], 3)?
            };
            if layout == 1 {
                let expected_large: Vec<_> = expected.iter().map(|value| value.map(i64::from)).collect();
                assert_eq!(arrow_result.array.as_any().downcast_ref::<Int64Array>().unwrap(),
                    &Int64Array::from(expected_large));
            } else {
                assert_eq!(arrow_result.array.as_any().downcast_ref::<Int32Array>().unwrap(),
                    &Int32Array::from(expected.to_vec()));
            }
            assert_arrays_eq!(&vortex_result,
                &PrimitiveArray::from_option_iter(expected).into_array(),
                &mut array_session().create_execution_ctx());
        }
    }
    Ok(())
}

#[test]
fn scalar_string_length_broadcasts_to_logical_rows() -> TestResult {
    let (mut arrow, _) = strings(&[Some("é")], 2);
    arrow.scalar = true;
    let vortex = ConstantArray::new("é", 4).into_array();
    let (arrow, vortex) = run(&BitLength, vec![(arrow, vortex)], 4)?;
    let expected = [Some(16); 4];
    assert_eq!(arrow.array.as_any().downcast_ref::<Int32Array>().unwrap(),
        &Int32Array::from(expected.to_vec()));
    assert_arrays_eq!(&vortex, &PrimitiveArray::from_iter([16i32; 4]).into_array(),
        &mut array_session().create_execution_ctx());
    Ok(())
}
