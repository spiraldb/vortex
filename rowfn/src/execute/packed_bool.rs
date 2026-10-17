// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Direct Boolean packing preserves the terminal and retry loop source shapes.

use rowfn_kernels::lane_kernels::IndexedSource;

use crate::{BatchBinding, BooleanOutput, FailureEvidence, HostResult, InputTuple, RowFn};
use super::{Execute, Mode, Outcome};

impl<H: BatchBinding, F: RowFn<H>> Execute<'_, H, F> {
    pub(super) fn boolean<Args, const MULTIVERSIONED: bool>(mut self,
        apply: impl Fn(Args::Elems<'_>) -> bool) -> HostResult<H, Outcome<H>>
    where Args: InputTuple<H>, H: BooleanOutput {
        if matches!(self.mode, Mode::Valid(_) | Mode::Filtered(_)) {
            return self.owned::<Args, bool, ()>(|_| (), |&(), args| apply(args));
        }
        self.validate::<Args>(H::output_type(), false, false)?;
        let columns = self.decode::<Args>()?.ok_or_else(|| H::error("dense decode cannot decline"))?;
        let source = Args::source(&columns, self.input_rows)?;
        if let Some(source) = Args::rows_source(&columns, self.input_rows)? {
            return H::collect_bool::<MULTIVERSIONED>(self.input_rows, |index| {
                // SAFETY: collect_bool visits only indices in the validated source domain.
                apply(unsafe { source.get_unchecked(index) })
            }, self.ctx).map(Outcome::Values);
        }
        H::collect_bool::<MULTIVERSIONED>(self.input_rows, |index| {
            // SAFETY: collect_bool visits only indices in the validated source domain.
            apply(unsafe { source.get_unchecked(index) })
        }, self.ctx).map(Outcome::Values)
    }

    pub(super) fn deferred_boolean<Args, Prepared, Fail, const MULTIVERSIONED: bool>(mut self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (bool, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Outcome<H>>
    where Args: InputTuple<H>, H: BooleanOutput, Fail: FailureEvidence {
        const { assert!(size_of::<Fail>() <= size_of::<bool>()); }
        if matches!(self.mode, Mode::Valid(_) | Mode::Filtered(_)) {
            return self.deferred::<Args, bool, Prepared, Fail>(prepare, apply, finish);
        }
        self.validate::<Args>(H::output_type(), true, true)?;
        if matches!(self.mode, Mode::Attempt) {
            return self.boolean_attempt::<Args, Prepared, Fail, MULTIVERSIONED>(prepare, apply, finish);
        }

        // Keep this terminal loop separate from the retry loop below. The original Vortex source
        // records that factoring their shared state changes LLVM's optimized dense kernel.
        let columns = self.decode::<Args>()?.ok_or_else(|| H::error("dense decode cannot decline"))?;
        let prepared = prepare(Args::constants(&columns));
        let source = Args::source(&columns, self.input_rows)?;
        let mut failure = Fail::default();
        let collect = |index| {
            // SAFETY: the collector visits only the validated source domain.
            let elements = unsafe { source.get_unchecked(index) };
            let (value, row_failure) = apply(&prepared, elements);
            failure |= row_failure;
            value
        };
        let values = H::collect_bool::<MULTIVERSIONED>(self.input_rows, collect, self.ctx)?;
        finish(failure)?;
        Ok(Outcome::Values(values))
    }

    fn boolean_attempt<Args, Prepared, Fail, const MULTIVERSIONED: bool>(mut self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (bool, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Outcome<H>>
    where Args: InputTuple<H>, H: BooleanOutput, Fail: FailureEvidence {
        let columns = self.decode::<Args>()?.ok_or_else(|| H::error("dense decode cannot decline"))?;
        let prepared = prepare(Args::constants(&columns));
        let source = Args::source(&columns, self.input_rows)?;

        // NB: capture one mutable borrow of the combined state. Separate field captures lose the
        // alias information required by the multiversioned loop in the multi-CGU, no-LTO baseline.
        let mut state = DeferredBoolState { source, prepared, apply, failure: Fail::default() };
        let state_ref = &mut state;
        let collect = move |index| {
            let state = &mut *state_ref;
            // SAFETY: the collector visits only the validated source domain.
            let elements = unsafe { state.source.get_unchecked(index) };
            let (value, row_failure) = (state.apply)(&state.prepared, elements);
            state.failure |= row_failure;
            value
        };
        let values = H::collect_bool::<MULTIVERSIONED>(self.input_rows, collect, self.ctx)?;
        match finish(state.failure) {
            Ok(()) => Ok(Outcome::Values(values)),
            Err(error) => Ok(Outcome::Deferred(error)),
        }
    }
}

struct DeferredBoolState<Source, Prepared, Apply, Fail> {
    source: Source,
    prepared: Prepared,
    apply: Apply,
    failure: Fail,
}
