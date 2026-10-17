// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Valid-row preparation followed by statically dispatched Boolean collection.

use rowfn_kernels::lane_kernels::IndexedSource;

use crate::{BatchBinding, BooleanOutput, FailureEvidence, HostResult, InputTuple};
use crate::{OutputBuffer, PreparedRows, RowFn, Selection};
use crate::prepare::PreparationMode;

use super::{Execute, Mode, Outcome, selected_rows};

impl<H: BatchBinding, F: RowFn<H>> Execute<'_, H, F> {
    pub(super) fn batch_prepared_boolean<Args, Prepared, Fail, const MULTIVERSIONED: bool>(
        mut self,
        prepare: impl for<'a> FnOnce(PreparedRows<'a, H, Args>) -> HostResult<H, Prepared>,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (bool, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>,
    ) -> HostResult<H, Outcome<H>>
    where
        Args: InputTuple<H>,
        H: BooleanOutput,
        Fail: FailureEvidence,
    {
        const { assert!(size_of::<Fail>() <= size_of::<bool>()); }
        self.validate::<Args>(H::output_type(), true, false)?;
        let Some(columns) = self.decode::<Args>()? else { return Ok(Outcome::Unsupported); };

        let preparation_mode = match &self.mode {
            Mode::Dense => PreparationMode::Dense,
            Mode::Valid(selection) => PreparationMode::Valid(*selection),
            Mode::Filtered(selection) => PreparationMode::Filtered(*selection),
            Mode::Attempt => return Err(H::error("batch preparation requires valid-only policy")),
        };
        let prepared = {
            let source = Args::source(&columns, self.input_rows)?;
            let rows = PreparedRows::new(
                &columns, source, preparation_mode, self.input_rows, self.output_rows,
            )?;
            prepare(rows)?
        };
        let source = Args::source(&columns, self.input_rows)?;

        if matches!(self.mode, Mode::Dense) {
            let mut failure = Fail::default();
            let values = H::collect_bool::<MULTIVERSIONED>(self.input_rows, |index| {
                // SAFETY: the collector visits only indices in the validated source domain.
                let (value, row_failure) = apply(&prepared, unsafe { source.get_unchecked(index) });
                failure |= row_failure;
                value
            }, self.ctx)?;
            finish(failure)?;

            return Ok(Outcome::Values(values));
        }

        if let Mode::Valid(selection) = &self.mode {
            let mut valid = vec![0u8; self.output_rows];
            selection.for_each(|index| valid[index] = 1);
            let mut failure = Fail::default();
            let values = H::collect_bool::<MULTIVERSIONED>(self.output_rows, |index| {
                // SAFETY: the Boolean collector only supplies indices below output_rows, which
                // is also the validated mask and source length in the valid-only mode.
                if unsafe { *valid.get_unchecked(index) } == 0 {
                    return false;
                }
                // SAFETY: a set mask byte came from the validated selection for this source.
                let (value, row_failure) = apply(&prepared, unsafe { source.get_unchecked(index) });
                failure |= row_failure;
                value
            }, self.ctx)?;
            finish(failure)?;

            return Ok(Outcome::Values(values));
        }

        let mut output = H::allocate(self.output_rows, self.ctx)?;
        let slots = &mut output.slots()[..self.output_rows];
        for slot in slots.iter_mut() { slot.write(false); }
        let mut failure = Fail::default();
        selected_rows::<H>(&self.mode, self.input_rows, self.output_rows, |input, output| {
            // SAFETY: selected_rows bounds input by the validated source length.
            let (value, row_failure) = apply(&prepared, unsafe { source.get_unchecked(input) });
            slots[output].write(value);
            failure |= row_failure;
            Ok(())
        })?;
        finish(failure)?;
        // SAFETY: placeholders and selected writes initialized every published slot.
        Ok(Outcome::Values(unsafe { output.finish(self.output_rows) }))
    }
}
