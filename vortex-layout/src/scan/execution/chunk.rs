// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Cuts a scan's rows into chunks and plans each one.

use std::ops::Range;
use std::sync::Arc;
use std::vec::IntoIter;

use rustc_hash::FxHashSet;
use vortex_error::VortexResult;
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
use super::SegmentLocation;
use super::filter::hint;
use super::filter::segments_read;
use super::selection::plan_selection;

/// The row grid every chunk boundary lies on.
pub(crate) const ROW_BLOCK_SIZE: usize = 8192;
/// The rows of a projection chunk.
pub(crate) const MAX_ROWS_PER_CHUNK: usize = 8 * ROW_BLOCK_SIZE;
/// The rows of a filtered chunk: large, so a conjunct's reads coalesce and its mask is cheap.
pub(crate) const FILTERED_ROWS_PER_CHUNK: usize = 16 * MAX_ROWS_PER_CHUNK;
const TARGET_BYTES_PER_CHUNK: u64 = 8 * 1024 * 1024;

/// Limit estimated bytes per chunk without changing the writer-aligned row granularity.
pub(crate) fn rows_per_chunk(row_count: u64, locations: &[SegmentLocation]) -> usize {
    if row_count <= MAX_ROWS_PER_CHUNK as u64 {
        return MAX_ROWS_PER_CHUNK;
    }
    let bytes: u64 = locations
        .iter()
        .map(|location| u64::from(location.length))
        .sum();
    let bytes_per_row = bytes.div_ceil(row_count).max(1);
    let blocks = TARGET_BYTES_PER_CHUNK / bytes_per_row / ROW_BLOCK_SIZE as u64;
    blocks.clamp(1, 8) as usize * ROW_BLOCK_SIZE
}

/// Chunks of `rows_per_chunk` rows from the start of `rows`, the last one shorter. Cut from
/// the range's own start rather than a grid over the file, a partition shorter than a chunk is
/// one chunk, so its reads go out together.
pub(crate) fn row_chunks(rows: Range<u64>, rows_per_chunk: usize) -> Vec<Range<u64>> {
    if rows.is_empty() {
        return Vec::new();
    }
    (rows.start..rows.end)
        .step_by(rows_per_chunk)
        .map(|start| start..start.saturating_add(rows_per_chunk as u64).min(rows.end))
        .collect()
}

/// Emits one planner per chunk of a scan's rows, in row order, each spawned as independent so
/// the driver may run it beside the others. A chunk none of whose rows is selected and kept is
/// never planned.
pub struct ChunkPlanner {
    scan: Arc<PreparedScan>,
    rows: Range<u64>,
    chunks: IntoIter<Range<u64>>,
    selection: Selection,
    /// The rows zone pruning kept, over `rows`, or `None` when nothing was pruned.
    kept: Option<Mask>,
    /// Whether the projection's reads were started, for a scan without a filter.
    prefetched: bool,
    /// Ids for hints, counting down from `u32::MAX`.
    next_hint: u32,
}

impl ChunkPlanner {
    /// The planner of the chunks of `rows`, planning only what `selection` and `kept`, the rows
    /// zone pruning left over `rows`, leave something of.
    pub fn new(
        scan: Arc<PreparedScan>,
        rows: Range<u64>,
        selection: Selection,
        kept: Option<Mask>,
    ) -> Self {
        let chunks = row_chunks(rows.clone(), scan.chunk_rows).into_iter();
        Self {
            scan,
            rows,
            chunks,
            selection,
            kept,
            prefetched: false,
            next_hint: u32::MAX,
        }
    }
}

impl IoConsumer for ChunkPlanner {
    fn set_io_result(&mut self, _request: IoRequestId, _result: IoResult) {}
}

impl Planner for ChunkPlanner {
    fn state(&self) -> State {
        if self.chunks.as_slice().is_empty() {
            State::Done
        } else {
            State::NeedsCompute
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        // A scan without a filter reads every projection segment of its rows: started together
        // here, before any chunk, they coalesce as one scan's reads would, instead of each chunk
        // reading its own as it starts.
        if !self.prefetched {
            self.prefetched = true;
            if self.scan.filter.is_none() && matches!(self.selection, Selection::All) {
                let len = usize::try_from(self.rows.end - self.rows.start).unwrap_or(usize::MAX);
                let segments = segments_read(
                    &self.scan.session,
                    std::iter::once(&self.scan.projection),
                    self.rows.clone(),
                    &Mask::new_true(len),
                    &self.scan.values,
                )?;
                let mut batch = Vec::new();
                let mut seen = FxHashSet::default();
                hint(
                    &self.scan.locations,
                    &mut self.next_hint,
                    IoIntent::Prefetch,
                    &segments,
                    &mut seen,
                    &mut batch,
                );
                if !batch.is_empty() {
                    self.scan
                        .metrics
                        .speculated_requests
                        .add(batch.len() as u64);
                    return Ok(PlannerOutput::NeedsIO(batch));
                }
            }
        }
        let kept = self.kept.as_ref().map(|kept| (kept, self.rows.start));
        for chunk in self.chunks.by_ref() {
            let scope = WorkScope {
                file_ordinal: 0,
                rows: chunk,
            };
            if let Some((scope, planner)) = plan_selection(&self.scan, scope, &self.selection, kept)
            {
                self.scan.metrics.chunks.add(1);
                return Ok(PlannerOutput::Spawn(scope, planner));
            }
            self.scan.metrics.chunks_skipped.add(1);
        }
        Ok(PlannerOutput::Done)
    }
}
