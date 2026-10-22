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
use vortex_scan::selection::Selection;

use super::PreparedScan;
use super::PruneNext;
use super::chunk::row_chunks;
use super::filter::hint;
use super::filter::segments_read;
use super::selection::selected_rows;
use crate::segments::SegmentId;

/// The segments one chunk of a scan reads: its predicate columns' and its projection's.
pub struct ChunkReads {
    pub rows: Range<u64>,
    pub predicates: Vec<SegmentId>,
    pub projection: Vec<SegmentId>,
}

impl ChunkReads {
    /// The reads of the chunks of `rows`, listed by compiling each chunk's plans once.
    pub(super) fn list(scan: &PreparedScan, rows: Range<u64>) -> VortexResult<Vec<Self>> {
        row_chunks(rows, scan.chunk_rows)
            .into_iter()
            .map(|chunk| {
                let len = usize::try_from(chunk.end - chunk.start).unwrap_or(usize::MAX);
                let mask = Mask::new_true(len);
                let predicates = match &scan.filter {
                    Some(filter) => segments_read(
                        &scan.session,
                        filter.predicates.iter(),
                        chunk.clone(),
                        &mask,
                        &scan.values,
                    )?,
                    None => Vec::new(),
                };
                let projection = segments_read(
                    &scan.session,
                    std::iter::once(&scan.projection),
                    chunk.clone(),
                    &mask,
                    &scan.values,
                )?;
                Ok(Self {
                    rows: chunk,
                    predicates,
                    projection,
                })
            })
            .collect()
    }
}

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
    /// Every chunk's reads, listed by the planner before this one while it waited, or here.
    chunk_reads: Vec<ChunkReads>,
    next: Option<PruneNext>,
    issued: bool,
    /// Ids for hints, counting down from `u32::MAX`.
    next_hint: u32,
}

impl ReadAheadPlanner {
    /// The planner reading ahead `window` chunks of `rows`, of which pruning kept `kept`,
    /// over `chunk_reads` if the planner before it listed them, continuing into `next`.
    pub fn new(
        scan: Arc<PreparedScan>,
        rows: Range<u64>,
        kept: Option<Mask>,
        window: usize,
        chunk_reads: Vec<ChunkReads>,
        next: PruneNext,
    ) -> Self {
        Self {
            scan,
            rows,
            kept,
            window,
            chunk_reads,
            next: Some(next),
            issued: false,
            next_hint: u32::MAX,
        }
    }

    /// The reads of the first window of surviving chunks, and the announcements of the next.
    /// A scan selecting rows by index reads nothing it was not asked for.
    fn batch(&mut self) -> VortexResult<IoBatch> {
        if !matches!(self.scan.selection, Selection::All) {
            return Ok(Vec::new());
        }
        if self.chunk_reads.is_empty() {
            self.chunk_reads = ChunkReads::list(&self.scan, self.rows.clone())?;
        }
        let scan = &self.scan;
        let kept = self.kept.as_ref().map(|kept| (kept, self.rows.start));
        let surviving = self
            .chunk_reads
            .iter()
            .filter(|chunk| {
                selected_rows(
                    WorkScope {
                        file_ordinal: 0,
                        rows: chunk.rows.clone(),
                    },
                    &scan.selection,
                    kept,
                )
                .is_some()
            })
            .take(self.window.saturating_mul(2).max(1));
        let mut batch = Vec::new();
        let mut seen = FxHashSet::default();
        let mut projections = Vec::new();
        for (index, chunk) in surviving.enumerate() {
            let intent = if index < self.window {
                IoIntent::Prefetch
            } else {
                IoIntent::Announce
            };
            hint(
                &scan.locations,
                &mut self.next_hint,
                intent,
                &chunk.predicates,
                &mut seen,
                &mut batch,
            );
            projections.push((intent, &chunk.projection));
        }
        // The projection's columns after every chunk's predicates, so the predicate reads are
        // dispatched first.
        for (intent, projection) in projections {
            hint(
                &scan.locations,
                &mut self.next_hint,
                intent,
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
            next(self.kept.take(), std::mem::take(&mut self.chunk_reads)),
        ))
    }
}
