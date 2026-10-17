// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Sink traversal preserves exact-row initialization evidence and safe abandonment.

use rowfn_kernels::lane_kernels::IndexedSource;

use crate::{BatchBinding, HostResult, InputTuple, RowFn};
use crate::sink::{OutputSink, SinkResult};
use super::{Execute, Mode, Outcome, selected_rows};

impl<H: BatchBinding, F: RowFn<H>> Execute<'_, H, F> {
    pub(super) fn sink<Args, Sink, Prepared, R>(mut self, params: Sink::Params,
        prepare: impl FnOnce(Args::ConstElems<'_>) -> Prepared,
        apply: impl Fn(&Prepared, Args::Elems<'_>, Sink::Row<'_>) -> R) -> HostResult<H, Outcome<H>>
    where Args: InputTuple<H>, Sink: OutputSink<H>, R: SinkResult<H::Error, WriteToken = Sink::WriteToken> {
        self.validate::<Args>(Sink::storage_type(&params)?, !R::INFALLIBLE, false)?;
        let Some(columns) = self.decode::<Args>()? else { return Ok(Outcome::Unsupported); };
        let prepared = prepare(Args::constants(&columns));
        let source = Args::source(&columns, self.input_rows)?;
        let mut sink = Sink::allocate(self.output_rows, &params, self.ctx)?;
        {
            let mut rows = sink.rows();
            if Sink::len(&rows) != self.output_rows { return Err(H::error("sink row count mismatch")); }
            match self.mode {
                Mode::Dense | Mode::Attempt => {
                    for index in 0..self.input_rows {
                        // SAFETY: source and this exact sink view cover the full row domain.
                        let args = unsafe { source.get_unchecked(index) };
                        // SAFETY: index is below the checked sink length.
                        let row = unsafe { Sink::row(&mut rows, index) };
                        apply(&prepared, args, row).into_result()?;
                    }
                }
                _ => {
                    Sink::initialize(&mut rows);
                    if Sink::len(&rows) != self.output_rows { return Err(H::error("sink initializer changed row count")); }
                    selected_rows::<H>(&self.mode, self.input_rows, self.output_rows, |input, output| {
                        // SAFETY: selected_rows checked both indices against these retained views.
                        let args = unsafe { source.get_unchecked(input) };
                        // SAFETY: output is below the sink length rechecked after initialization.
                        let row = unsafe { Sink::row(&mut rows, output) };
                        apply(&prepared, args, row).into_result()
                    })?;
                }
            }
        }
        // SAFETY: each visited callback returned its required token. Skipped rows were initialized.
        unsafe { sink.finish(self.ctx) }.map(Outcome::Values)
    }
}
