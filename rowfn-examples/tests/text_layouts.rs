// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Shared string dispatch selects independent concrete layouts for both operands.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::BooleanArray;
use arrow_array::LargeStringArray;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_schema::ArrowError;
use arrow_schema::Field;
use rowfn::RowFn;
use rowfn_arrow::ArrowHost;
use rowfn_arrow::ArrowOperand;
use rowfn_arrow::Output;
use rowfn_examples::Concat;
use rowfn_examples::Like;
use rowfn_examples::NoOptions;
use rowfn_examples::StringPredicate;
use rowfn_examples::Trim;
use rstest::rstest;

fn strings(layout: usize, values: &[Option<&str>], scalar: bool) -> ArrowOperand {
    let column: ArrayRef = match layout {
        0 => Arc::new(StringArray::from(values.to_vec())),
        1 => Arc::new(LargeStringArray::from(values.to_vec())),
        _ => Arc::new(StringViewArray::from(values.to_vec())),
    };
    ArrowOperand {
        dtype: Field::new("text", column.data_type().clone(), true),
        column,
        scalar,
    }
}

fn run<F>(function: &F, inputs: Vec<ArrowOperand>, rows: usize) -> Result<Output, ArrowError>
where
    F: RowFn<ArrowHost, Options = NoOptions>,
{
    let types = inputs.iter().map(|input| input.dtype.clone()).collect::<Vec<_>>();
    let output = rowfn_arrow::plan(function, &NoOptions, &types)?;
    rowfn_arrow::invoke(function, &NoOptions, &inputs, rows, output)
}

#[rstest]
fn concat_preserves_slices_nulls_and_ownership_across_layout_pairs(
    #[values(0, 1, 2)] left: usize,
    #[values(0, 1, 2)] right: usize,
) -> Result<(), ArrowError> {
    let mut lhs = strings(
        left,
        &[Some("discard"), Some("é"), None, Some("a long prefix")],
        false,
    );
    let mut rhs = strings(
        right,
        &[Some("discard"), Some(" tail"), Some("ignored"), Some(" and suffix")],
        false,
    );
    lhs.column = lhs.column.slice(1, 3);
    rhs.column = rhs.column.slice(1, 3);
    let result = run(&Concat, vec![lhs, rhs], 3)?;
    let expected = StringViewArray::from(vec![
        Some("é tail"),
        None,
        Some("a long prefix and suffix"),
    ]);
    assert_eq!(result.array.as_any().downcast_ref::<StringViewArray>().unwrap(), &expected);

    Ok(())
}

#[rstest]
fn predicates_prepare_constants_across_layout_pairs(
    #[values(0, 1, 2)] left: usize,
    #[values(0, 1, 2)] right: usize,
) -> Result<(), ArrowError> {
    let input = strings(left, &[Some("éclair"), None, Some("arrow"), Some("")], false);
    let pattern = strings(right, &[Some("é")], true);
    let result = run(&StringPredicate::StartsWith, vec![input.clone(), pattern], 4)?;
    let expected = BooleanArray::from(vec![Some(true), None, Some(false), Some(false)]);
    assert_eq!(result.array.as_any().downcast_ref::<BooleanArray>().unwrap(), &expected);

    let pattern = strings(right, &[Some("é%")], true);
    let result = run(&Like::<false, false>, vec![input, pattern], 4)?;
    assert_eq!(result.array.as_any().downcast_ref::<BooleanArray>().unwrap(), &expected);

    Ok(())
}

#[rstest]
fn trim_retains_output_after_sliced_input_owners_drop(
    #[values(0, 1, 2)] layout: usize,
) -> Result<(), ArrowError> {
    let mut input = strings(
        layout,
        &[Some("discard"), Some("  é  "), None, Some("  a longer string  ")],
        false,
    );
    input.column = input.column.slice(1, 3);
    let result = run(&Trim, vec![input], 3)?;
    let expected = StringViewArray::from(vec![Some("é"), None, Some("a longer string")]);
    assert_eq!(result.array.as_any().downcast_ref::<StringViewArray>().unwrap(), &expected);

    Ok(())
}
