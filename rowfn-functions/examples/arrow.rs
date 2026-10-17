// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Invoke shared functions on Arrow without enabling any Vortex bindings.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::Int64Array;
use arrow_array::StringArray;
use arrow_schema::ArrowError;
use arrow_schema::Field;
use rowfn_arrow::ArrowOperand;
use rowfn_functions::Add;
use rowfn_functions::NoOptions;
use rowfn_functions::Trim;

fn operand(column: ArrayRef, name: &str, scalar: bool) -> ArrowOperand {
    ArrowOperand {
        dtype: Field::new(name, column.data_type().clone(), column.null_count() != 0),
        column,
        scalar,
    }
}

fn main() -> Result<(), ArrowError> {
    let inputs = [
        operand(Arc::new(Int64Array::from(vec![Some(1), None, Some(41)])), "lhs", false),
        operand(Arc::new(Int64Array::from(vec![1])), "rhs", true),
    ];
    let fields = inputs.iter().map(|input| input.dtype.clone()).collect::<Vec<_>>();
    let output = rowfn_arrow::plan(&Add::<true>, &NoOptions, &fields)?;
    let result = rowfn_arrow::invoke(&Add::<true>, &NoOptions, &inputs, 3, output)?;
    println!("checked addition: {:?}", result.array);

    let input = operand(
        Arc::new(StringArray::from(vec![Some("  hello  "), None, Some("  a longer string  ")])),
        "text",
        false,
    );
    let output = rowfn_arrow::plan(&Trim, &NoOptions, &[input.dtype.clone()])?;
    let result = rowfn_arrow::invoke(&Trim, &NoOptions, &[input], 3, output)?;
    // The temporary input operand is gone. The Utf8View result owns its output bytes.
    println!("trimmed strings: {:?}", result.array);
    Ok(())
}
