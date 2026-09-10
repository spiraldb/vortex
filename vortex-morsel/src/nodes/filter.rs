// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::IntoArray;
use vortex_array::dtype::DType;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::demand::RowDomain;
use crate::node::Batch;
use crate::node::Child;
use crate::node::Cx;
use crate::node::LookAhead;
use crate::node::Operator;
use crate::node::Step;
use crate::node::concat_parts;
use crate::node::filter_rows;

/// A `Filter` operator, placed by the plan over one leaf of a filtered subtree.
///
/// The mask it applies is produced once per morsel by the root's mask child and buffered in the
/// context. This operator reads its own rows out of that buffer with its own cursor, hands the
/// selection down as the hint so the leaf can skip a read nobody wants, and keeps exactly the
/// selected rows of what comes back. A selection that keeps nothing returns empty without
/// touching the leaf at all.
pub struct FilterExec {
    input: Child,
    /// Which mask buffer in the context to read.
    tee: usize,
    /// Root row of the input's first row.
    root_offset: u64,
    dtype: DType,

    registered: bool,
    domain: RowDomain,
    state: State,
}

enum State {
    PullMask {
        /// Root coordinate of the next mask row this filter needs.
        cursor: u64,
        /// Pieces received so far, as local ranges within this filter's rows.
        masks: Vec<(Range<usize>, Mask)>,
    },
    PullInput {
        masks: Vec<(Range<usize>, Mask)>,
    },
    Emitted,
}

impl FilterExec {
    /// Build a filter of `input`, whose rows start at root row `root_offset`, by mask buffer
    /// `tee`.
    pub fn new(
        input: Child,
        tee: usize,
        root_offset: u64,
        dtype: DType,
        domain: RowDomain,
    ) -> Self {
        let cursor = root_offset + domain.range().start;
        Self {
            input,
            tee,
            root_offset,
            dtype,
            registered: false,
            domain,
            state: State::PullMask {
                cursor,
                masks: Vec::new(),
            },
        }
    }

    fn coverage(&self) -> Range<u64> {
        self.root_offset + self.domain.range().start..self.root_offset + self.domain.range().end
    }
}

impl Operator for FilterExec {
    fn row_domain(&self) -> &RowDomain {
        &self.domain
    }

    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        if !self.registered {
            cx.tees[self.tee].register(self.coverage());
            self.registered = true;
        }
        self.input.look_ahead(cx)
    }

    fn next(&mut self, _hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step> {
        let coverage = self.coverage();
        loop {
            match &mut self.state {
                State::Emitted => return Ok(Step::Finished),
                State::PullMask { cursor, masks } => {
                    if *cursor == coverage.end {
                        let masks = std::mem::take(masks);
                        if masks.iter().all(|(_, mask)| mask.all_false()) {
                            cx.stats()
                                .record_skipped(masks.iter().map(|(range, _)| range.len()).sum());
                            self.state = State::Emitted;
                            return Ok(Step::Batch(Batch::array(
                                coverage,
                                Canonical::empty(&self.dtype).into_array(),
                            )));
                        }
                        self.state = State::PullInput { masks };
                        continue;
                    }
                    match cx.tees[self.tee].serve(*cursor, coverage.end)? {
                        Some(batch) => {
                            let lo = usize::try_from(batch.coverage.start - coverage.start)
                                .vortex_expect("filter offset fits usize");
                            let hi = usize::try_from(batch.coverage.end - coverage.start)
                                .vortex_expect("filter offset fits usize");
                            masks.push((lo..hi, batch.value.into_mask()?));
                            *cursor = batch.coverage.end;
                        }
                        None => {
                            // The root fills the buffer before it pulls the body, so this only
                            // happens if the mask producer ended early.
                            return Err(vortex_err!(
                                "filter {} needs mask rows from {cursor} that were never produced",
                                self.input.id()
                            ));
                        }
                    }
                }
                State::PullInput { masks } => {
                    // The leaf only tests the hint for all-false, and that case never reaches it.
                    let rows = usize::try_from(self.domain.range().end - self.domain.range().start)
                        .vortex_expect("filter range fits usize");
                    match self.input.next(&Mask::new_true(rows), cx)? {
                        Step::Batch(batch) => {
                            let array =
                                filter_by_pieces(batch.value.into_array()?, masks, &self.dtype)?;
                            self.state = State::Emitted;
                            return Ok(Step::Batch(Batch::array(coverage, array)));
                        }
                        Step::Blocked => return Ok(Step::Blocked),
                        Step::Finished => {
                            return Err(vortex_err!(
                                "filter input {} produced no value",
                                self.input.id()
                            ));
                        }
                    }
                }
            }
        }
    }

    fn close(&mut self, cx: &mut Cx<'_>) {
        self.input.close(cx);
        self.state = State::Emitted;
    }

    fn describe(&self) -> String {
        format!(
            "Filter(node {} by mask buffer {})",
            self.input.id(),
            self.tee
        )
    }
}

/// Slice the input per mask piece, keep each piece's selected rows, concatenate.
fn filter_by_pieces(
    array: ArrayRef,
    pieces: &[(Range<usize>, Mask)],
    dtype: &DType,
) -> VortexResult<ArrayRef> {
    if let [(range, mask)] = pieces
        && range.start == 0
        && range.end == array.len()
    {
        return filter_rows(array, mask.clone());
    }
    let mut parts = Vec::with_capacity(pieces.len());
    for (range, mask) in pieces {
        if mask.all_false() {
            continue;
        }
        let part = array.slice(range.clone())?;
        parts.push(filter_rows(part, mask.clone())?);
    }
    concat_parts(parts, dtype)
}
