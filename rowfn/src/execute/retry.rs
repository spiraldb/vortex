// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! A separate dense owned loop preserves retry's compiler-sensitive constant specialization.

use rowfn_kernels::lane_kernels::{IndexedSource, IndexedSourceExt};

use crate::{BatchBinding, FailureEvidence, HostResult, InputTuple, OutputBinding, OutputBuffer, RowFn};
use super::{Execute, Outcome};

impl<H: BatchBinding, F: RowFn<H>> Execute<'_, H, F> {
    pub(super) fn owned_attempt<Args, Out, Prepared, Fail>(mut self,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>) -> (Out, Fail),
        finish: impl FnOnce(Fail) -> HostResult<H, ()>) -> HostResult<H, Outcome<H>>
    where Args: InputTuple<H>, Out: Default + 'static, H: OutputBinding<Out>, Fail: FailureEvidence {
        const {
            assert!(!std::mem::needs_drop::<Out>());
            assert!(size_of::<Fail>() <= size_of::<Out>());
        }

        // Keep this loop separate from terminal collection. Factoring them together changes the
        // existing collector's optimized kernel under multiple CGUs without LTO.
        let columns = self.decode::<Args>()?.ok_or_else(|| H::error("dense decode cannot decline"))?;
        let prepared = prepare(Args::constants(&columns));
        let mut output = H::allocate(self.output_rows, self.ctx)?;
        let slots = &mut output.slots()[..self.output_rows];

        let failure = if let Some(source) = Args::rows_source(&columns, self.input_rows)? {
            source.map_checked_into(slots, |args| apply(&prepared, args))
        } else {
            // Keep length validation local to the constant branch. Iterating over slots also
            // avoids retaining an output bound check through an address-taken row-count value.
            let source = Args::source(&columns, self.input_rows)?;
            let mut failure = Fail::default();
            for (index, slot) in slots.iter_mut().enumerate() {
                // SAFETY: dense attempts use equal input/output lengths, validated by the source.
                let (value, row_failure) = apply(&prepared, unsafe { source.get_unchecked(index) });
                slot.write(value);
                failure |= row_failure;
            }
            failure
        };

        match finish(failure) {
            Ok(()) => {
                // SAFETY: either branch initialized the complete published prefix.
                Ok(Outcome::Values(unsafe { output.finish(self.output_rows) }))
            }
            Err(error) => Ok(Outcome::Deferred(error)),
        }
    }
}
