// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Typed visitor entry points select the corresponding collection loop.

use crate::{BatchBinding, BooleanOutput, FailureEvidence, HostResult, InputTuple, OutputBinding, PreparedRows, RowFn, RowVisitor};
use crate::sink::{OutputSink, SinkResult};
use super::{Execute, Outcome};

impl<H: BatchBinding, F: RowFn<H>> RowVisitor<H> for Execute<'_, H, F> {
    type VisitResult = Outcome<H>;
    fn with_output_type(mut self, dtype: H::NativeType) -> Self { self.label = Some(dtype); self }
    fn visit_prepared<Args, Out, Prepared>(self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> Out) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out> {
        self.owned::<Args, Out, Prepared>(prepare, apply)
    }
    fn visit_prepared_into<Args, Sink, Prepared, R>(self, params: Sink::Params,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>, Sink::Row<'_>) -> R) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, Sink: OutputSink<H>, R: SinkResult<H::Error, WriteToken = Sink::WriteToken> {
        self.sink::<Args, Sink, Prepared, R>(params, prepare, apply)
    }
    fn visit_prepared_deferred<Args, Out, Prepared, Fail>(self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (Out, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out>, Fail: FailureEvidence {
        self.deferred::<Args, Out, Prepared, Fail>(prepare, apply, finish)
    }
    fn visit_bool<Args, const MULTIVERSIONED: bool>(self, apply: impl Fn(Args::Elems<'_>) -> bool)
        -> HostResult<H, Self::VisitResult> where Args: InputTuple<H>, H: BooleanOutput {
        self.boolean::<Args, MULTIVERSIONED>(apply)
    }
    fn visit_prepared_deferred_bool<Args, Prepared, Fail, const MULTIVERSIONED: bool>(self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (bool, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, H: BooleanOutput, Fail: FailureEvidence {
        self.deferred_boolean::<Args, Prepared, Fail, MULTIVERSIONED>(prepare, apply, finish)
    }

    fn visit_batch_prepared_deferred_bool<Args, Prepared, Fail, const MULTIVERSIONED: bool>(self,
        prepare: impl for<'a> FnOnce(PreparedRows<'a, H, Args>) -> HostResult<H, Prepared>,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (bool, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Self::VisitResult>
    where Args: InputTuple<H>, H: BatchBinding + BooleanOutput, Fail: FailureEvidence {
        self.batch_prepared_boolean::<Args, Prepared, Fail, MULTIVERSIONED>(prepare, apply, finish)
    }
}
