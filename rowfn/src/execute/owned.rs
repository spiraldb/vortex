// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Owned-output loops write directly into the host's stable allocation.

use rowfn_kernels::lane_kernels::{IndexedSource, IndexedSourceExt};

use crate::{BatchBinding, FailureEvidence, HostResult, InputTuple, OutputBinding, OutputBuffer, RowFn};
use super::{Execute, Mode, Outcome, selected_rows};

impl<H: BatchBinding, F: RowFn<H>> Execute<'_, H, F> {
    pub(super) fn owned<Args, Out, Prepared>(mut self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> Out) -> HostResult<H, Outcome<H>>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out> {
        const { assert!(!std::mem::needs_drop::<Out>()); }
        self.validate::<Args>(H::output_type(), false, false)?;
        let Some(columns) = self.decode::<Args>()? else { return Ok(Outcome::Unsupported); };
        let prepared = prepare(Args::constants(&columns));
        if matches!(self.mode, Mode::Dense | Mode::Attempt) {
            if let Some(source) = Args::rows_source(&columns, self.input_rows)? {
                return H::build_from(source, |args| apply(&prepared, args), self.ctx).map(Outcome::Values);
            }
            let source = Args::source(&columns, self.input_rows)?;
            return H::build_from(source, |args| apply(&prepared, args), self.ctx).map(Outcome::Values);
        }
        let source = Args::source(&columns, self.input_rows)?;
        let mut output = H::allocate(self.output_rows, self.ctx)?;
        let slots = &mut output.slots()[..self.output_rows];
        for slot in slots.iter_mut() { slot.write(Out::default()); }
        selected_rows::<H>(&self.mode, self.input_rows, self.output_rows, |input, output| {
            // SAFETY: selected_rows bounds input by the validated source length.
            slots[output].write(apply(&prepared, unsafe { source.get_unchecked(input) }));
            Ok(())
        })?;
        // SAFETY: placeholders and selected writes initialized every published slot.
        Ok(Outcome::Values(unsafe { output.finish(self.output_rows) }))
    }

    pub(super) fn deferred<Args, Out, Prepared, Fail>(mut self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (Out, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Outcome<H>>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out>, Fail: FailureEvidence {
        const {
            assert!(!std::mem::needs_drop::<Out>());
            assert!(size_of::<Fail>() <= size_of::<Out>());
        }
        self.validate::<Args>(H::output_type(), true, true)?;
        if matches!(self.mode, Mode::Attempt) {
            return self.owned_attempt::<Args, Out, Prepared, Fail>(prepare, apply, finish);
        }
        let Some(columns) = self.decode::<Args>()? else { return Ok(Outcome::Unsupported); };
        let prepared = prepare(Args::constants(&columns));
        let source = Args::source(&columns, self.input_rows)?;
        let mut output = H::allocate(self.output_rows, self.ctx)?;
        let slots = &mut output.slots()[..self.output_rows];
        let failure = match self.mode {
            Mode::Dense | Mode::Attempt => source.map_checked_into(slots, |args| apply(&prepared, args)),
            _ => {
                for slot in slots.iter_mut() { slot.write(Out::default()); }
                let mut failure = Fail::default();
                selected_rows::<H>(&self.mode, self.input_rows, self.output_rows, |input, output| {
                    // SAFETY: selected_rows bounds input by the validated source length.
                    let (value, failed) = apply(&prepared, unsafe { source.get_unchecked(input) });
                    slots[output].write(value);
                    failure |= failed;
                    Ok(())
                })?;
                failure
            }
        };
        finish(failure)?;
        // SAFETY: every slot is initialized, and deferred evidence was accepted.
        Ok(Outcome::Values(unsafe { output.finish(self.output_rows) }))
    }
}
