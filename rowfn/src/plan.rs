// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Semantic validation and nullable policy selected before any row callback.

use std::marker::PhantomData;

use crate::{BatchBinding, BooleanOutput, FailureEvidence, HostResult, InputTuple, OutputBinding, PreparedRows, RowFn, RowVisitor, TypeBinding};
use crate::sink::{OutputSink, SinkResult};
use crate::visitor::private;

/// Output metadata and execution policy selected by a typed visit.
pub struct Plan<H: TypeBinding> {
    pub(crate) storage: H::NativeType,
    pub(crate) output: H::NativeType,
    pub(crate) policy: Policy,
}

impl<H: TypeBinding> Plan<H> {
    /// Complete result metadata, including strict outer nullability.
    pub fn output_type(&self) -> &H::NativeType { &self.output }

    pub(crate) fn new(storage: H::NativeType, label: Option<H::NativeType>,
        types: &[H::NativeType], policy: Policy) -> HostResult<H, Self> {
        if H::nullable(&storage) { return Err(H::error("row output storage must be non-nullable")); }
        let output = label.unwrap_or_else(|| storage.clone());
        if H::nullable(&output) { return Err(H::error("row output label must be non-nullable")); }
        H::validate_label(&storage, &output)?;
        let output = H::with_nullable(&output, types.iter().any(H::nullable));

        Ok(Self { storage, output, policy })
    }

    pub(crate) fn require_same(&self, actual: &Self) -> HostResult<H, ()> {
        if self.storage != actual.storage || self.output != actual.output || self.policy != actual.policy {
            return Err(H::error("row dispatch must reproduce the planned output and policy"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Policy { Dense, Retry, ValidOnly }

pub(crate) fn policy<H: TypeBinding, Args: InputTuple<H>>(fallible: bool, deferred: bool) -> Policy {
    if !Args::DENSE_SAFE || !Args::DECODE_INFALLIBLE { return Policy::ValidOnly; }
    if deferred { Policy::Retry } else if fallible { Policy::ValidOnly } else { Policy::Dense }
}

/// Plan a call without decoding inputs or evaluating row callbacks.
pub fn plan<H: TypeBinding, F: RowFn<H>>(function: &F, options: &F::Options,
    types: &[H::NativeType]) -> HostResult<H, Plan<H>> {
    if types.len() != F::ARG_NAMES.len() { return Err(H::error("row function argument count mismatch")); }
    function.dispatch(options, types, Planner::<H, F> { types, label: None, marker: PhantomData })
}

pub(crate) fn validate<H: TypeBinding, F: RowFn<H>, Args: InputTuple<H>>(types: &[H::NativeType], fallible: bool)
    -> HostResult<H, ()> {
    if Args::ARITY != F::ARG_NAMES.len() { return Err(H::error("typed row signature arity mismatch")); }
    if F::INFALLIBLE && fallible { return Err(H::error("infallible function selected a fallible visitor")); }
    Args::validate(types)
}

struct Planner<'a, H: TypeBinding, F> {
    types: &'a [H::NativeType],
    label: Option<H::NativeType>,
    marker: PhantomData<F>,
}
impl<H: TypeBinding, F> private::Sealed for Planner<'_, H, F> {}
impl<H: TypeBinding, F: RowFn<H>> RowVisitor<H> for Planner<'_, H, F> {
    type VisitResult = Plan<H>;
    fn with_output_type(mut self, dtype: H::NativeType) -> Self { self.label = Some(dtype); self }
    fn visit_prepared<Args, Out, Prepared>(self, _: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        _: impl Fn(&Prepared, Args::Elems<'_>) -> Out) -> HostResult<H, Plan<H>>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out> {
        const { assert!(!std::mem::needs_drop::<Out>()); }
        validate::<H, F, Args>(self.types, false)?;
        Plan::new(H::output_type(), self.label, self.types, policy::<H, Args>(false, false))
    }
    fn visit_prepared_into<Args, Sink, Prepared, R>(self, params: Sink::Params,
        _: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        _: impl Fn(&Prepared, Args::Elems<'_>, Sink::Row<'_>) -> R) -> HostResult<H, Plan<H>>
    where Args: InputTuple<H>, Sink: OutputSink<H>, R: SinkResult<H::Error, WriteToken = Sink::WriteToken> {
        validate::<H, F, Args>(self.types, !R::INFALLIBLE)?;
        Plan::new(Sink::storage_type(&params)?, self.label, self.types, policy::<H, Args>(!R::INFALLIBLE, false))
    }
    fn visit_prepared_deferred<Args, Out, Prepared, Fail>(self,
        _: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        _: impl Fn(&Prepared, Args::Elems<'_>) -> (Out, Fail),
        _: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Plan<H>>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out>, Fail: FailureEvidence {
        const {
            assert!(!std::mem::needs_drop::<Out>());
            assert!(size_of::<Fail>() <= size_of::<Out>());
        }
        validate::<H, F, Args>(self.types, true)?;
        Plan::new(H::output_type(), self.label, self.types, policy::<H, Args>(true, true))
    }
    fn visit_bool<Args, const MULTIVERSIONED: bool>(self, apply: impl Fn(Args::Elems<'_>) -> bool)
        -> HostResult<H, Plan<H>> where Args: InputTuple<H>, H: BooleanOutput {
        self.visit::<Args, bool>(apply)
    }
    fn visit_prepared_deferred_bool<Args, Prepared, Fail, const MULTIVERSIONED: bool>(self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (bool, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Plan<H>>
    where Args: InputTuple<H>, H: BooleanOutput, Fail: FailureEvidence {
        self.visit_prepared_deferred::<Args, bool, Prepared, Fail>(prepare, apply, finish)
    }

    fn visit_batch_prepared_deferred_bool<Args, Prepared, Fail, const MULTIVERSIONED: bool>(self,
        _: impl for<'a> FnOnce(PreparedRows<'a, H, Args>) -> HostResult<H, Prepared>,
        _: impl Fn(&Prepared, Args::Elems<'_>) -> (bool, Fail),
        _: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Plan<H>>
    where Args: InputTuple<H>, H: BatchBinding + BooleanOutput, Fail: FailureEvidence {
        const { assert!(size_of::<Fail>() <= size_of::<bool>()); }
        validate::<H, F, Args>(self.types, true)?;
        Plan::new(H::output_type(), self.label, self.types, Policy::ValidOnly)
    }
}
