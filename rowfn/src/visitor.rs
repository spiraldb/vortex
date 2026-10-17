// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Visitor-style dispatch shared by function authors across hosts.

use crate::{BatchBinding, BooleanOutput, FailureEvidence, Host, HostResult, InputTuple, OutputBinding};
use crate::PreparedRows;
use crate::sink::{OutputSink, SinkResult};

/// A strict scalar function with one semantic dispatch implementation per supported host family.
///
/// Dispatch must depend only on options and native input types. Valid inputs cannot produce null.
/// Callbacks must not panic or have side effects outside their supplied output row. Preparation is
/// invocation-local and can run again after a rejected dense attempt. No evaluation count is
/// promised. IDs, serialization, sessions, and optimizer registration belong to host wrappers.
pub trait RowFn<H: Host>: Clone + Send + Sync + 'static {
    /// Function-owned options, independent of host registration.
    type Options;
    /// Exact argument names and arity.
    const ARG_NAMES: &'static [&'static str];
    /// Semantic row infallibility, excluding decoding and infrastructure failures.
    const INFALLIBLE: bool;
    /// Select typed inputs and output using the function's semantic rules.
    fn dispatch<V: RowVisitor<H>>(&self, options: &Self::Options, args: &[H::NativeType], visitor: V)
        -> HostResult<H, V::VisitResult>;
}

/// A planning or execution visit at statically selected input and output types.
pub trait RowVisitor<H: Host>: Sized + private::Sealed {
    /// A plan or host-native batch output, never a per-row dynamic value.
    type VisitResult;
    /// Declare a non-nullable semantic label that preserves storage values without conversion.
    fn with_output_type(self, dtype: H::NativeType) -> Self;
    /// Return one infallible owned value per row.
    fn visit<Args, Out>(self, apply: impl Fn(Args::Elems<'_>) -> Out) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out> {
        self.visit_prepared::<Args, Out, ()>(|_| (), move |&(), args| apply(args))
    }
    /// Prepare from this invocation's constants before returning owned values.
    fn visit_prepared<Args, Out, Prepared>(self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> Out) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out>;
    /// Write through the supplied sink row. Success must prove initialization of that exact row.
    fn visit_into<Args, Sink, R>(self, params: Sink::Params,
        apply: impl Fn(Args::Elems<'_>, Sink::Row<'_>) -> R) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, Sink: OutputSink<H>, R: SinkResult<H::Error, WriteToken = Sink::WriteToken> {
        self.visit_prepared_into::<Args, Sink, (), R>(params, |_| (), move |&(), args, row| apply(args, row))
    }
    /// Prepare once for a sink traversal. A retry creates a new prepared value.
    fn visit_prepared_into<Args, Sink, Prepared, R>(self, params: Sink::Params,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>, Sink::Row<'_>) -> R) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, Sink: OutputSink<H>, R: SinkResult<H::Error, WriteToken = Sink::WriteToken>;
    /// Return values and OR-reduced evidence. Only rejection by `finish` can trigger dense retry.
    fn visit_deferred<Args, Out, Fail>(self, apply: impl Fn(Args::Elems<'_>) -> (Out, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out>, Fail: FailureEvidence {
        self.visit_prepared_deferred::<Args, Out, (), Fail>(|_| (), move |&(), args| apply(args), finish)
    }
    /// Prepare before collecting values with deferred failure evidence.
    fn visit_prepared_deferred<Args, Out, Prepared, Fail>(self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (Out, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out>, Fail: FailureEvidence;
    /// Pack Boolean output directly. Multiversioning is intended for cheap predicates.
    fn visit_bool<Args, const MULTIVERSIONED: bool>(self, apply: impl Fn(Args::Elems<'_>) -> bool)
        -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, H: BooleanOutput;
    /// Pack Boolean values and accumulate deferred failure evidence.
    fn visit_deferred_bool<Args, Fail, const MULTIVERSIONED: bool>(self,
        apply: impl Fn(Args::Elems<'_>) -> (bool, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, H: BooleanOutput, Fail: FailureEvidence {
        self.visit_prepared_deferred_bool::<Args, (), Fail, MULTIVERSIONED>(
            |_| (), move |&(), args| apply(args), finish)
    }
    /// Prepared direct Boolean packing with deferred failure evidence.
    fn visit_prepared_deferred_bool<Args, Prepared, Fail, const MULTIVERSIONED: bool>(self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (bool, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, H: BooleanOutput, Fail: FailureEvidence;

    /// Prepare owned state from valid decoded rows before direct Boolean collection.
    ///
    /// This method selects valid rows before preparation and skips preparation for all-null
    /// batches. A semantic error from a visited valid row is therefore terminal without null-based
    /// retry. Decode and infrastructure errors remain terminal too. Other visitor calls keep their
    /// existing traversal policy.
    fn visit_batch_prepared_deferred_bool<Args, Prepared, Fail, const MULTIVERSIONED: bool>(self,
        prepare: impl for<'a> FnOnce(PreparedRows<'a, H, Args>) -> HostResult<H, Prepared>,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (bool, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, H: BatchBinding + BooleanOutput, Fail: FailureEvidence;
}

pub(crate) mod private { pub trait Sealed {} }
