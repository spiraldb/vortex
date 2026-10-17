// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Arrow-owned input and output bindings for strict `rowfn` functions.
//!
//! The invocation boundary retains fields, explicit scalar markers, logical batch length, and the
//! planned output field. Dictionary inputs are materialized over referenced keys before decoding.
//! This crate has no Vortex dependency, including its tests.
//!
//! A decoded owner's borrowed view cannot escape its lifetime:
//!
//! ```compile_fail
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int64Array};
//! use rowfn::InputBinding;
//! use rowfn_arrow::ArrowHost;
//!
//! fn escape() -> &'static [i64] {
//!     let input: ArrayRef = Arc::new(Int64Array::from(vec![1]));
//!     let decoded = <ArrowHost as InputBinding<i64>>::decode(&input, false, &mut ()).unwrap();
//!     <ArrowHost as InputBinding<i64>>::view(&decoded)
//! }
//! ```

#![deny(missing_docs)]

use arrow_array::ArrayRef;
use arrow_schema::{ArrowError, Field};
use rowfn::{Host, Operand, RowFn};

mod batch;
pub use batch::logical_field;
pub use batch::materialize;

mod input;
pub use input::ArrowPrimitive;

mod output;

mod string;

/// Local marker for Arrow's native storage and field metadata.
#[derive(Clone, Copy, Debug)]
pub struct ArrowHost;
impl Host for ArrowHost {
    type Column = ArrayRef;
    type NativeType = Field;
    type Context = ();
    type Error = ArrowError;
    fn error(message: &str) -> ArrowError { ArrowError::InvalidArgumentError(message.into()) }
}

/// An explicit Arrow operand, including its complete input field.
pub type ArrowOperand = Operand<ArrowHost>;

/// The output array and its planned semantic field.
pub struct Output {
    /// Arrow-owned result storage.
    pub array: ArrayRef,
    /// Complete result metadata, preserved even when no valid row executes.
    pub field: Field,
}

/// Infer the output field, normalizing dictionary storage without discarding field metadata.
pub fn plan<F: RowFn<ArrowHost>>(function: &F, options: &F::Options, inputs: &[Field]) -> Result<Field, ArrowError> {
    let types = inputs.iter().map(logical_field).collect::<Result<Vec<_>, _>>()?;
    Ok(rowfn::plan::<ArrowHost, F>(function, options, &types)?.output_type().clone())
}

/// Invoke a shared function directly on Arrow, with no implicit length-one broadcasting.
///
/// Dictionary materialization preserves key nulls and null dictionary values. Row functions only
/// see referenced logical rows, so unused dictionary values cannot introduce row errors.
pub fn invoke<F: RowFn<ArrowHost>>(function: &F, options: &F::Options, inputs: &[ArrowOperand],
    rows: usize, output: Field) -> Result<Output, ArrowError> {
    let inputs = inputs.iter().map(|input| {
        if input.column.data_type() != input.dtype.data_type() {
            return Err(ArrowHost::error("input field must describe its Arrow array"));
        }
        Ok(ArrowOperand { column: materialize(&input.column)?, dtype: logical_field(&input.dtype)?, scalar: input.scalar })
    }).collect::<Result<Vec<_>, ArrowError>>()?;
    let array = rowfn::execute::<ArrowHost, F>(function, options, &inputs, rows, &output, &mut ())?;
    Ok(Output { array, field: output })
}
