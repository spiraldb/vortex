// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Batch routing around statically dispatched row traversal.
//!
//! Only rejected deferred evidence enters validity-based retry. Decoder, allocation, validation,
//! and publication errors return directly. Dense success never asks for a materialized selection.

use std::marker::PhantomData;

use crate::{BatchBinding, HostResult, InputTuple, Operand, Plan, RowFn, Selection, ValiditySummary, plan};
use crate::plan::Policy;
use crate::visitor::private;

mod owned;
mod batch_prepared;
mod packed_bool;
mod retry;
mod sink;
mod visitor;

/// Execute a planned strict function over explicit operands and a logical batch length.
///
/// The caller supplies planned output metadata. It must equal the function's inferred output,
/// including nullability and semantic metadata. Each invocation prepares its own decoded owners.
pub fn execute<H: BatchBinding, F: RowFn<H>>(function: &F, options: &F::Options,
    inputs: &[Operand<H>], rows: usize, output: &H::NativeType, ctx: &mut H::Context)
    -> HostResult<H, H::Column> {
    for input in inputs { H::validate_operand(input, rows)?; }
    let types: Vec<_> = inputs.iter().map(|input| input.dtype.clone()).collect();
    let planned = plan::<H, F>(function, options, &types)?;
    if planned.output != *output { return Err(H::error("planned output metadata must match row dispatch")); }
    let validity = H::validity(inputs, rows, ctx)?;
    let summary = H::validity_summary(&validity);
    if rows != 0 && summary == ValiditySummary::None {
        return H::all_null(output, rows, ctx);
    }

    let all_scalar = rows != 0 && !inputs.is_empty() && inputs.iter().all(|input| input.scalar)
        && summary == ValiditySummary::All;
    let input_rows = if all_scalar { 1 } else { rows };
    let mode = if summary == ValiditySummary::All || rows == 0 || inputs.is_empty() {
        Mode::Dense
    } else {
        match planned.policy {
            Policy::Dense => Mode::Dense,
            Policy::Retry => Mode::Attempt,
            Policy::ValidOnly => return selected::<H, F>(function, options, inputs, rows, &types,
                &planned, &validity, None, ctx),
        }
    };
    let outcome = run::<H, F>(function, options, inputs, input_rows, input_rows, &types, &planned, mode, ctx)?;
    match outcome {
        Outcome::Values(values) => {
            let values = if all_scalar { H::broadcast(values, rows, ctx)? } else { values };
            H::publish(values, &planned.storage, output, &validity, rows, ctx)
        }
        Outcome::Deferred(error) => selected::<H, F>(function, options, inputs, rows, &types,
            &planned, &validity, Some(error), ctx),
        Outcome::Unsupported => Err(H::error("dense decoding cannot decline")),
    }
}

fn selected<H: BatchBinding, F: RowFn<H>>(function: &F, options: &F::Options,
    inputs: &[Operand<H>], rows: usize, types: &[H::NativeType], planned: &Plan<H>,
    validity: &H::Validity, deferred: Option<H::Error>, ctx: &mut H::Context)
    -> HostResult<H, H::Column> {
    let selection = H::selection(validity, rows, ctx)?;
    if selection.len() != rows || selection.count() > rows {
        return Err(H::error("selection must cover the original row domain"));
    }
    if selection.count() == rows {
        if let Some(error) = deferred { return Err(error); }
        let outcome = run::<H, F>(function, options, inputs, rows, rows, types, planned, Mode::Dense, ctx)?;
        return publish::<H>(outcome, planned, validity, rows, ctx);
    }
    if selection.count() == 0 { return H::all_null(&planned.output, rows, ctx); }
    drop(deferred);

    let outcome = run::<H, F>(function, options, inputs, rows, rows, types, planned, Mode::Valid(&selection), ctx)?;
    if !matches!(outcome, Outcome::Unsupported) {
        return publish::<H>(outcome, planned, validity, rows, ctx);
    }
    let filtered = inputs.iter().map(|input| H::filter(input, &selection, ctx))
        .collect::<HostResult<H, Vec<_>>>()?;
    let outcome = run::<H, F>(function, options, &filtered, selection.count(), rows, types, planned,
        Mode::Filtered(&selection), ctx)?;
    publish::<H>(outcome, planned, validity, rows, ctx)
}

fn publish<H: BatchBinding>(outcome: Outcome<H>, planned: &Plan<H>, validity: &H::Validity,
    rows: usize, ctx: &mut H::Context) -> HostResult<H, H::Column> {
    match outcome {
        Outcome::Values(values) => H::publish(values, &planned.storage, &planned.output, validity, rows, ctx),
        Outcome::Deferred(error) => Err(error),
        Outcome::Unsupported => Err(H::error("filtered decoding cannot decline")),
    }
}

fn run<H: BatchBinding, F: RowFn<H>>(function: &F, options: &F::Options, inputs: &[Operand<H>],
    input_rows: usize, output_rows: usize, types: &[H::NativeType], planned: &Plan<H>,
    mode: Mode<'_, H>, ctx: &mut H::Context) -> HostResult<H, Outcome<H>> {
    function.dispatch(options, types, Execute::<H, F> {
        inputs, input_rows, output_rows, types, planned, mode, ctx, label: None, marker: PhantomData,
    })
}

enum Mode<'a, H: BatchBinding> { Dense, Attempt, Valid(&'a H::Selection), Filtered(&'a H::Selection) }

enum Outcome<H: BatchBinding> { Values(H::Column), Deferred(H::Error), Unsupported }

struct Execute<'a, H: BatchBinding, F> {
    inputs: &'a [Operand<H>],
    input_rows: usize,
    output_rows: usize,
    types: &'a [H::NativeType],
    planned: &'a Plan<H>,
    mode: Mode<'a, H>,
    ctx: &'a mut H::Context,
    label: Option<H::NativeType>,
    marker: PhantomData<F>,
}
impl<H: BatchBinding, F> private::Sealed for Execute<'_, H, F> {}

impl<H: BatchBinding, F: RowFn<H>> Execute<'_, H, F> {
    fn validate<Args: InputTuple<H>>(&self, storage: H::NativeType, fallible: bool, deferred: bool)
        -> HostResult<H, ()> {
        plan::validate::<H, F, Args>(self.types, fallible)?;
        let actual = Plan::new(storage, self.label.clone(), self.types,
            plan::policy::<H, Args>(fallible, deferred))?;
        self.planned.require_same(&actual)
    }

    fn decode<Args: InputTuple<H>>(&mut self) -> HostResult<H, Option<Args::Columns>> {
        let tolerant = matches!(self.mode, Mode::Valid(_));
        if tolerant && !Args::can_decode(self.inputs)? { return Ok(None); }
        Args::decode(self.inputs, tolerant, self.ctx).map(Some)
    }
}

/// Resolve selection shape once, then use the selection's unsafe contract for each row.
fn selected_rows<H: BatchBinding>(mode: &Mode<'_, H>, input_rows: usize, output_rows: usize,
    mut visit: impl FnMut(usize, usize) -> HostResult<H, ()>) -> HostResult<H, ()> {
    match mode {
        Mode::Valid(selection) => {
            if selection.len() != input_rows || input_rows != output_rows {
                return Err(H::error("valid selection must match the input and output row counts"));
            }
            selection.try_for_each(|index| visit(index, index))
        }
        Mode::Filtered(selection) => {
            if selection.len() != output_rows || selection.count() != input_rows {
                return Err(H::error("filtered selection must match the input and output row counts"));
            }
            let mut input = 0;
            selection.try_for_each(|output| {
                let result = visit(input, output);
                input += 1;
                result
            })
        }
        _ => unreachable!("selected traversal requires a selection"),
    }
}
