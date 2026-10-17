// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Experimental Vortex bindings for the independent `rowfn` executor.
//!
//! The existing `row` module remains the comparison implementation. Registration uses the normal
//! scalar-function registry through [`VortexRowFn`], with no new built-in catalog entry.

use ::rowfn::{Host, Operand};
use vortex_error::{VortexError, VortexResult, vortex_err};

use crate::{ArrayRef, ExecutionCtx};
use crate::dtype::DType;

mod batch;
mod input;
mod output;

mod registration;
pub use registration::VortexRowFn;

/// Local marker on which Vortex implements portable binding capabilities.
#[derive(Clone, Copy, Debug)]
pub struct VortexHost;

impl Host for VortexHost {
    type Column = ArrayRef;
    type NativeType = DType;
    type Context = ExecutionCtx;
    type Error = VortexError;
    fn error(message: &str) -> VortexError { vortex_err!("{message}") }
}

/// Retain a Vortex input and classify constants through the existing encoding-aware helper.
pub fn operand(column: ArrayRef) -> Operand<VortexHost> {
    let scalar = !column.is_empty() && super::row::constant_input(&column).is_some();
    Operand { dtype: column.dtype().clone(), column, scalar }
}

/// Retain the value beneath a recognized constant wrapper before decoding its first row.
///
/// A masked constant can have a null first row and valid later rows. Decoding the masked first row
/// would sanitize UTF-8 to an empty string and lose the constant used by those valid rows.
pub fn decoded_input(column: &ArrayRef, scalar: bool) -> VortexResult<ArrayRef> {
    if !scalar { return Ok(column.clone()); }
    let constant = super::row::constant_input(column)
        .ok_or_else(|| vortex_err!("scalar input must retain a recognized constant encoding"))?;
    constant.slice(0..1)
}

/// Execute shared dispatch using Vortex's native input and planned output metadata.
pub fn execute<F: ::rowfn::RowFn<VortexHost>>(function: &F, options: &F::Options,
    inputs: &[ArrayRef], rows: usize, output: &DType, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
    let operands: Vec<_> = inputs.iter().cloned().map(operand).collect();
    ::rowfn::execute::<VortexHost, F>(function, options, &operands, rows, output, ctx)
}

#[cfg(test)]
mod tests;
