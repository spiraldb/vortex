// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Borrowed valid rows for invocation-local preparation.
//!
//! A function may build owned lookup state from decoded inputs before its row callback runs.
//! The view borrows retained decoded owners and visits only rows where every input is valid.
//! Prepared state cannot borrow from this view because the visitor requires one owned result type.

use rowfn_kernels::lane_kernels::IndexedSource;

use crate::{BatchBinding, HostResult, InputTuple, Selection};

pub(crate) enum PreparationMode<'a, H: BatchBinding> {
    Dense,
    Valid(&'a H::Selection),
    Filtered(&'a H::Selection),
}

/// Valid decoded input rows supplied to a batch preparation callback.
///
/// The row index passed to [`Self::try_for_each_valid`] is in the original logical batch.
/// This value and its borrowed row elements cannot outlive the callback invocation.
pub struct PreparedRows<'a, H: BatchBinding, Args: InputTuple<H>> {
    columns: &'a Args::Columns,
    source: Args::Source<'a>,
    mode: PreparationMode<'a, H>,
    input_rows: usize,
}

impl<'a, H: BatchBinding, Args: InputTuple<H>> PreparedRows<'a, H, Args> {
    pub(crate) fn new(
        columns: &'a Args::Columns,
        source: Args::Source<'a>,
        mode: PreparationMode<'a, H>,
        input_rows: usize,
        output_rows: usize,
    ) -> HostResult<H, Self> {
        if source.len() != input_rows {
            return Err(H::error("prepared input source must match the input row count"));
        }
        match &mode {
            PreparationMode::Dense if input_rows != output_rows => {
                return Err(H::error("dense preparation must match the output row count"));
            }
            PreparationMode::Valid(selection)
                if selection.len() != output_rows || selection.len() != input_rows => {
                return Err(H::error("valid preparation must match the row domain"));
            }
            PreparationMode::Filtered(selection)
                if selection.len() != output_rows || selection.count() != input_rows => {
                return Err(H::error("filtered preparation must match the selected row count"));
            }
            _ => {}
        }

        Ok(Self { columns, source, mode, input_rows })
    }

    /// Return the number of rows whose input values can affect this invocation's result.
    pub fn valid_len(&self) -> usize {
        match &self.mode {
            PreparationMode::Dense => self.input_rows,
            PreparationMode::Valid(selection) | PreparationMode::Filtered(selection) => {
                selection.count()
            }
        }
    }

    /// Borrow explicit scalar operands only when at least one row is valid.
    ///
    /// With no valid rows, a scalar's stored payload may itself be null and must not be inspected.
    pub fn constants(&self) -> Option<Args::ConstElems<'_>> {
        (self.valid_len() != 0).then(|| Args::constants(self.columns))
    }

    /// Visit decoded valid rows once in logical row order, stopping at the first error.
    ///
    /// The callback may construct owned state, but it must not retain borrowed row values.
    pub fn try_for_each_valid<E>(
        &self,
        mut visit: impl FnMut(usize, Args::Elems<'a>) -> Result<(), E>,
    ) -> Result<(), E> {
        match &self.mode {
            PreparationMode::Dense => {
                for index in 0..self.input_rows {
                    // SAFETY: construction validated the source against the input row count.
                    visit(index, unsafe { self.source.get_unchecked(index) })?;
                }
                Ok(())
            }
            PreparationMode::Valid(selection) => selection.try_for_each(|index| {
                // SAFETY: the selection contract bounds each index by its validated domain.
                visit(index, unsafe { self.source.get_unchecked(index) })
            }),
            PreparationMode::Filtered(selection) => {
                let mut input = 0;
                selection.try_for_each(|output| {
                    // SAFETY: the selection count equals the source length, and each callback
                    // advances the input index once before the next selected row.
                    let value = unsafe { self.source.get_unchecked(input) };
                    input += 1;
                    visit(output, value)
                })
            }
        }
    }
}
