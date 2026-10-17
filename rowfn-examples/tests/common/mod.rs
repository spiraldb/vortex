// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Cross-host fixtures live outside both independent crates.

#![allow(dead_code)] // Each integration test uses a different subset of the shared fixtures.

use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, Int64Array};
use arrow_buffer::NullBuffer;
use arrow_schema::Field;
use rowfn::RowFn;
use rowfn_arrow::{ArrowHost, ArrowOperand};
use rowfn_examples::NoOptions;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute, array_session};
use vortex_array::arrays::{ConstantArray, PrimitiveArray};
use vortex_array::scalar_fn::unstable::rowfn::{self as adapter, VortexHost};
use vortex_array::validity::Validity;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn pair(values: &[i64], valid: &[bool], scalar: bool, rows: usize) -> (ArrowOperand, ArrayRef) {
    let arrow: ArrowArrayRef = Arc::new(Int64Array::new(values.to_vec().into(), Some(NullBuffer::from(valid.to_vec()))));
    let dtype = Field::new("input", arrow.data_type().clone(), true);
    let vortex = if scalar {
        let scalar = if valid[0] { Some(values[0]) } else { None };
        ConstantArray::new(scalar, rows).into_array()
    } else {
        PrimitiveArray::new(values.to_vec(), Validity::from_iter(valid.iter().copied())).into_array()
    };
    (ArrowOperand { column: arrow, dtype, scalar }, vortex)
}

pub fn run<F>(function: &F, inputs: Vec<(ArrowOperand, ArrayRef)>, rows: usize)
    -> TestResult<(rowfn_arrow::Output, ArrayRef)>
where F: RowFn<ArrowHost, Options = NoOptions> + RowFn<VortexHost, Options = NoOptions> {
    let (arrow, vortex): (Vec<_>, Vec<_>) = inputs.into_iter().unzip();
    let output = rowfn_arrow::plan(function, &NoOptions, &arrow.iter().map(|a| a.dtype.clone()).collect::<Vec<_>>())?;
    let arrow = rowfn_arrow::invoke(function, &NoOptions, &arrow, rows, output)?;
    let output = rowfn::plan::<VortexHost, F>(function, &NoOptions, &vortex.iter().map(|a| a.dtype().clone()).collect::<Vec<_>>())?;
    let vortex = adapter::execute(function, &NoOptions, &vortex, rows, output.output_type(), &mut array_session().create_execution_ctx())?;
    Ok((arrow, vortex))
}

pub fn errors<F>(
    function: &F,
    inputs: Vec<(ArrowOperand, ArrayRef)>,
    rows: usize,
) -> TestResult<(String, String)>
where
    F: RowFn<ArrowHost, Options = NoOptions> + RowFn<VortexHost, Options = NoOptions>,
{
    let (arrow, vortex): (Vec<_>, Vec<_>) = inputs.into_iter().unzip();
    let fields: Vec<_> = arrow.iter().map(|input| input.dtype.clone()).collect();
    let output = rowfn_arrow::plan(function, &NoOptions, &fields)?;
    let arrow = rowfn_arrow::invoke(function, &NoOptions, &arrow, rows, output)
        .err()
        .ok_or("expected an Arrow row error")?;

    let types: Vec<_> = vortex.iter().map(|input| input.dtype().clone()).collect();
    let output = rowfn::plan::<VortexHost, F>(function, &NoOptions, &types)?;
    let vortex = adapter::execute(
        function,
        &NoOptions,
        &vortex,
        rows,
        output.output_type(),
        &mut array_session().create_execution_ctx(),
    )
    .err()
    .ok_or("expected a Vortex row error")?;

    Ok((arrow.to_string(), vortex.to_string()))
}
