// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Reads ahead for the chunks of a scan's rows before any chunk is planned.

use std::ops::Range;
use std::sync::Arc;

use rustc_hash::FxHashSet;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_io::request::IoBatch;
use vortex_io::request::IoConsumer;
use vortex_io::request::IoIntent;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoResult;
use vortex_mask::Mask;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_scan::planning::planner::WorkScope;

use super::PreparedScan;
use super::PruneNext;
use super::chunk::row_chunks;
use super::filter::hint;
use super::filter::segments_read;
use super::selection::selected_rows;

/// Starts the reads of the first `window` chunks pruning and the selection leave something of,
/// their predicate columns and their projection's, and announces the next `window` chunks', so
/// the window's reads go out together and the next window's coalesce with them. Then emits
/// the planner it was built to continue into, with the kept rows unchanged.
///
/// A read is for whichever chunk fetches its segment: the service shares it with every session.
pub struct ReadAheadPlanner {
    scan: Arc<PreparedScan>,
    rows: Range<u64>,
    kept: Option<Mask>,
    window: usize,
    next: Option<PruneNext>,
    issued: bool,
    /// Ids for hints, counting down from `u32::MAX`.
    next_hint: u32,
}

impl ReadAheadPlanner {
    /// The planner reading ahead `window` chunks of `rows`, of which pruning kept `kept`,
    /// continuing into `next`.
    pub fn new(
        scan: Arc<PreparedScan>,
        rows: Range<u64>,
        kept: Option<Mask>,
        window: usize,
        next: PruneNext,
    ) -> Self {
        Self {
            scan,
            rows,
            kept,
            window,
            next: Some(next),
            issued: false,
            next_hint: u32::MAX,
        }
    }

    /// The reads of the first window, and the announcements of the next.
    fn batch(&mut self) -> VortexResult<IoBatch> {
        let scan = &self.scan;
        let kept = self.kept.as_ref().map(|kept| (kept, self.rows.start));
        let chunks = row_chunks(self.rows.clone(), scan.chunk_rows)
            .into_iter()
            .filter_map(|chunk| {
                selected_rows(
                    WorkScope {
                        file_ordinal: 0,
                        rows: chunk,
                    },
                    &scan.selection,
                    kept,
                )
            })
            .take(self.window.saturating_mul(2).max(1));
        let mut batch = Vec::new();
        let mut seen = FxHashSet::default();
        let mut projections = Vec::new();
        for (index, selected) in chunks.enumerate() {
            let intent = if index < self.window {
                IoIntent::Prefetch
            } else {
                IoIntent::Announce
            };
            let rows = selected.scope.rows.clone();
            if let Some(filter) = &scan.filter {
                let predicates = segments_read(
                    &scan.session,
                    filter.predicates.iter(),
                    rows.clone(),
                    &selected.mask,
                    &scan.values,
                )?;
                hint(
                    &scan.locations,
                    &mut self.next_hint,
                    intent,
                    &predicates,
                    &mut seen,
                    &mut batch,
                );
            }
            projections.push((
                intent,
                segments_read(
                    &scan.session,
                    std::iter::once(&scan.projection),
                    rows,
                    &selected.mask,
                    &scan.values,
                )?,
            ));
        }
        // The projection's columns after every chunk's predicates, so the predicate reads are
        // dispatched first.
        for (intent, projection) in &projections {
            hint(
                &scan.locations,
                &mut self.next_hint,
                *intent,
                projection,
                &mut seen,
                &mut batch,
            );
        }
        Ok(batch)
    }
}

impl IoConsumer for ReadAheadPlanner {
    fn set_io_result(&mut self, _request: IoRequestId, _result: IoResult) {}
}

impl Planner for ReadAheadPlanner {
    fn state(&self) -> State {
        if self.next.is_none() {
            State::Done
        } else {
            State::NeedsCompute
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        if !self.issued {
            self.issued = true;
            let batch = self.batch()?;
            if !batch.is_empty() {
                self.scan
                    .metrics
                    .read_ahead_requests
                    .add(batch.len() as u64);
                tracing::debug!(
                    row_start = self.rows.start,
                    row_end = self.rows.end,
                    requests = batch.len(),
                    "v2 read ahead"
                );
                return Ok(PlannerOutput::NeedsIO(batch));
            }
        }
        let next = self
            .next
            .take()
            .ok_or_else(|| vortex_err!("the read-ahead planner has nothing to continue into"))?;
        Ok(PlannerOutput::Planner(
            WorkScope {
                file_ordinal: 0,
                rows: self.rows.clone(),
            },
            next(self.kept.take()),
        ))
    }
}
