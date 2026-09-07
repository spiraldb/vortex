// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Per-run counters. The eval matrix in the prototype plan records these per row.

use std::time::Duration;

/// Work attributed to one completed morsel when detailed observability is enabled.
///
/// Segment reads are deduplicated scan-wide, so `segment_ids` records the exact logical segments
/// named by this morsel while physical request and byte totals remain scan-level counters.
#[derive(Clone, Debug, Default)]
pub struct MorselTrace {
    /// Stable index in scan output order.
    pub index: usize,
    /// Executor worker that drove the morsel.
    pub worker: usize,
    /// Inclusive root-row start.
    pub row_start: u64,
    /// Exclusive root-row end.
    pub row_end: u64,
    /// Rows selected by the initial demand mask.
    pub selected_rows: u64,
    /// Rows emitted after execution.
    pub output_rows: u64,
    /// Planning polls performed for this morsel.
    pub plan_polls: u64,
    /// Execution polls performed for this morsel.
    pub execute_polls: u64,
    /// Logical IO uses named by this morsel.
    pub io_uses: u64,
    /// New scan-wide segment request cells created while planning this morsel.
    pub io_requests: u64,
    /// Scheduler IO batches created while planning this morsel.
    pub io_batches: u64,
    /// Uses that found an existing scan-wide or morsel-local IO cell.
    pub io_cell_hits: u64,
    /// Unique cells registered in this morsel's IO plane.
    pub io_registered: u64,
    /// Inline non-blocking read attempts.
    pub nowait_attempts: u64,
    /// Inline non-blocking reads satisfied immediately.
    pub nowait_hits: u64,
    /// Inline non-blocking reads that would have blocked.
    pub nowait_misses: u64,
    /// Inline non-blocking reads unsupported by the source.
    pub nowait_unsupported: u64,
    /// Exact-ticket IO suspensions during execution.
    pub execute_io_blocks: u64,
    /// Exact segment IDs named by this morsel, sorted and deduplicated.
    pub segment_ids: Vec<u32>,
    /// Segment IDs decoded by this morsel, in execution order.
    pub decoded_segment_ids: Vec<u32>,
    /// Segment IDs reused from another morsel, in execution order.
    pub reused_segment_ids: Vec<u32>,
    /// Time spent polling plan nodes.
    pub planning_time: Duration,
    /// Time spent polling execute nodes.
    pub execution_time: Duration,
    /// Time this worker spent waiting for this morsel's exact IO dependencies.
    pub worker_io_wait_time: Duration,
    /// Time spent retiring node state.
    pub retire_time: Duration,
    /// Wall time from assignment through retirement.
    pub wall_time: Duration,
}

/// Counters accumulated by one driving thread, summed across threads at the end of a run.
#[derive(Clone, Debug, Default)]
pub struct ScanStats {
    /// Per-morsel work records, populated only when detailed observability is enabled.
    pub morsel_traces: Vec<MorselTrace>,
    /// Morsels driven.
    pub morsels: u64,
    /// Logical rows covered by all morsel ranges.
    pub morsel_rows: u64,
    /// Rows selected by the initial morsel demands.
    pub selected_rows: u64,
    /// Number of planning polls across all workers.
    pub plan_polls: u64,
    /// Number of execution polls across all workers.
    pub execute_polls: u64,
    /// IO uses named by planning streams.
    pub io_uses: u64,
    /// Reads actually issued to the segment source.
    pub io_requests: u64,
    /// Required/speculative scheduler batches containing those reads.
    pub io_batches: u64,
    /// Uses that found a cell already named inside the same morsel.
    pub io_cell_hits: u64,
    /// Uses that went through registration.
    pub io_registered: u64,
    /// Bytes returned by the segment source.
    pub io_bytes: u64,
    /// Reads handed out through the demand stream and answered by a completion, rather than
    /// resolved inline through the probe.
    pub io_waits: u64,
    /// Inline non-blocking read attempts made by execution.
    pub nowait_attempts: u64,
    /// Inline non-blocking reads satisfied immediately.
    pub nowait_hits: u64,
    /// Inline non-blocking reads that would have waited on storage.
    pub nowait_misses: u64,
    /// Inline non-blocking reads unsupported by the source or filesystem.
    pub nowait_unsupported: u64,
    /// Cumulative wall time handed-out reads spent between being started and being completed.
    ///
    /// Reads overlap, so this is not additive CPU or scan time.
    pub io_wait_time: Duration,
    /// Segment decodes performed.
    pub decodes: u64,
    /// Decodes served from a shared cell published by another morsel.
    pub decode_reuses: u64,
    /// Rows leaves handed up, before any selection was applied.
    pub rows_materialized: u64,
    /// Rows that survived the actual selection at the root.
    pub rows_selected: u64,
    /// Conjuncts skipped because the mask was already all-false.
    pub conjuncts_short_circuited: u64,
    /// Morsels whose filter selected no rows.
    pub morsels_empty: u64,
    /// Exact-ticket suspensions returned by execution nodes.
    pub execute_io_blocks: u64,
    /// Morsels that suspended at least once on IO.
    pub morsels_blocked_for_io: u64,
    /// Minimum logical IO uses named by one morsel.
    pub io_uses_per_morsel_min: Option<u64>,
    /// Maximum logical IO uses named by one morsel.
    pub io_uses_per_morsel_max: u64,
    /// Minimum new scan-wide segment requests created by one morsel.
    pub io_requests_per_morsel_min: Option<u64>,
    /// Maximum new scan-wide segment requests created by one morsel.
    pub io_requests_per_morsel_max: u64,
    /// Minimum scheduler IO batches created by one morsel.
    pub io_batches_per_morsel_min: Option<u64>,
    /// Maximum scheduler IO batches created by one morsel.
    pub io_batches_per_morsel_max: u64,
    /// Maximum exact-ticket suspensions returned by one morsel.
    pub io_blocks_per_morsel_max: u64,
    /// Time to the first batch emitted by this thread.
    pub time_to_first_batch: Option<Duration>,
    /// Scan setup time before workers start, populated when scan observability is enabled.
    pub setup_time: Duration,
    /// Sum of worker time inside plan-node polling, populated when scan observability is enabled.
    pub planning_time: Duration,
    /// Sum of worker time inside execute-node polling, populated when scan observability is enabled.
    pub execution_time: Duration,
    /// Sum of worker time waiting for exact I/O dependencies, populated when scan observability is enabled.
    pub worker_io_wait_time: Duration,
    /// Sum of worker time retiring node state, populated when scan observability is enabled.
    pub retire_time: Duration,
    /// End-to-end executor wall time, populated when scan observability is enabled.
    pub wall_time: Duration,
}

impl ScanStats {
    /// Fold another thread's counters into this one.
    pub fn merge(&mut self, other: &ScanStats) {
        self.morsel_traces
            .extend(other.morsel_traces.iter().cloned());
        self.morsels += other.morsels;
        self.morsel_rows += other.morsel_rows;
        self.selected_rows += other.selected_rows;
        self.plan_polls += other.plan_polls;
        self.execute_polls += other.execute_polls;
        self.io_uses += other.io_uses;
        self.io_requests += other.io_requests;
        self.io_batches += other.io_batches;
        self.io_cell_hits += other.io_cell_hits;
        self.io_registered += other.io_registered;
        self.io_bytes += other.io_bytes;
        self.io_waits += other.io_waits;
        self.nowait_attempts += other.nowait_attempts;
        self.nowait_hits += other.nowait_hits;
        self.nowait_misses += other.nowait_misses;
        self.nowait_unsupported += other.nowait_unsupported;
        self.io_wait_time += other.io_wait_time;
        self.decodes += other.decodes;
        self.decode_reuses += other.decode_reuses;
        self.rows_materialized += other.rows_materialized;
        self.rows_selected += other.rows_selected;
        self.conjuncts_short_circuited += other.conjuncts_short_circuited;
        self.morsels_empty += other.morsels_empty;
        self.execute_io_blocks += other.execute_io_blocks;
        self.morsels_blocked_for_io += other.morsels_blocked_for_io;
        self.io_uses_per_morsel_min =
            min_option(self.io_uses_per_morsel_min, other.io_uses_per_morsel_min);
        self.io_uses_per_morsel_max = self
            .io_uses_per_morsel_max
            .max(other.io_uses_per_morsel_max);
        self.io_requests_per_morsel_min = min_option(
            self.io_requests_per_morsel_min,
            other.io_requests_per_morsel_min,
        );
        self.io_requests_per_morsel_max = self
            .io_requests_per_morsel_max
            .max(other.io_requests_per_morsel_max);
        self.io_batches_per_morsel_min = min_option(
            self.io_batches_per_morsel_min,
            other.io_batches_per_morsel_min,
        );
        self.io_batches_per_morsel_max = self
            .io_batches_per_morsel_max
            .max(other.io_batches_per_morsel_max);
        self.io_blocks_per_morsel_max = self
            .io_blocks_per_morsel_max
            .max(other.io_blocks_per_morsel_max);
        self.time_to_first_batch = match (self.time_to_first_batch, other.time_to_first_batch) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.setup_time += other.setup_time;
        self.planning_time += other.planning_time;
        self.execution_time += other.execution_time;
        self.worker_io_wait_time += other.worker_io_wait_time;
        self.retire_time += other.retire_time;
        self.wall_time = self.wall_time.max(other.wall_time);
    }

    pub(crate) fn begin_morsel_trace(
        &mut self,
        index: usize,
        worker: usize,
        row_start: u64,
        row_end: u64,
        selected_rows: u64,
    ) {
        self.morsel_traces.push(MorselTrace {
            index,
            worker,
            row_start,
            row_end,
            selected_rows,
            plan_polls: self.plan_polls,
            execute_polls: self.execute_polls,
            io_uses: self.io_uses,
            io_requests: self.io_requests,
            io_batches: self.io_batches,
            io_cell_hits: self.io_cell_hits,
            io_registered: self.io_registered,
            nowait_attempts: self.nowait_attempts,
            nowait_hits: self.nowait_hits,
            nowait_misses: self.nowait_misses,
            nowait_unsupported: self.nowait_unsupported,
            execute_io_blocks: self.execute_io_blocks,
            planning_time: self.planning_time,
            execution_time: self.execution_time,
            worker_io_wait_time: self.worker_io_wait_time,
            retire_time: self.retire_time,
            ..MorselTrace::default()
        });
    }

    pub(crate) fn record_decode(&mut self, segment: u32) {
        self.decodes += 1;
        if let Some(trace) = self.morsel_traces.last_mut() {
            trace.decoded_segment_ids.push(segment);
        }
    }

    pub(crate) fn record_decode_reuse(&mut self, segment: u32) {
        self.decode_reuses += 1;
        if let Some(trace) = self.morsel_traces.last_mut() {
            trace.reused_segment_ids.push(segment);
        }
    }

    pub(crate) fn finish_morsel_trace(
        &mut self,
        index: usize,
        output_rows: u64,
        segment_ids: Vec<u32>,
        wall_time: Duration,
    ) {
        let Some(trace) = self.morsel_traces.last_mut() else {
            return;
        };
        debug_assert_eq!(trace.index, index);
        trace.output_rows = output_rows;
        trace.segment_ids = segment_ids;
        trace.plan_polls = self.plan_polls - trace.plan_polls;
        trace.execute_polls = self.execute_polls - trace.execute_polls;
        trace.io_uses = self.io_uses - trace.io_uses;
        trace.io_requests = self.io_requests - trace.io_requests;
        trace.io_batches = self.io_batches - trace.io_batches;
        trace.io_cell_hits = self.io_cell_hits - trace.io_cell_hits;
        trace.io_registered = self.io_registered - trace.io_registered;
        trace.nowait_attempts = self.nowait_attempts - trace.nowait_attempts;
        trace.nowait_hits = self.nowait_hits - trace.nowait_hits;
        trace.nowait_misses = self.nowait_misses - trace.nowait_misses;
        trace.nowait_unsupported = self.nowait_unsupported - trace.nowait_unsupported;
        trace.execute_io_blocks = self.execute_io_blocks - trace.execute_io_blocks;
        trace.planning_time = self.planning_time - trace.planning_time;
        trace.execution_time = self.execution_time - trace.execution_time;
        trace.worker_io_wait_time = self.worker_io_wait_time - trace.worker_io_wait_time;
        trace.retire_time = self.retire_time - trace.retire_time;
        trace.wall_time = wall_time;
    }

    /// Record the scheduling shape of one completed morsel.
    pub(crate) fn record_morsel_io(&mut self, uses: u64, requests: u64, batches: u64, blocks: u64) {
        self.io_uses_per_morsel_min = min_option(self.io_uses_per_morsel_min, Some(uses));
        self.io_uses_per_morsel_max = self.io_uses_per_morsel_max.max(uses);
        self.io_requests_per_morsel_min =
            min_option(self.io_requests_per_morsel_min, Some(requests));
        self.io_requests_per_morsel_max = self.io_requests_per_morsel_max.max(requests);
        self.io_batches_per_morsel_min = min_option(self.io_batches_per_morsel_min, Some(batches));
        self.io_batches_per_morsel_max = self.io_batches_per_morsel_max.max(batches);
        self.io_blocks_per_morsel_max = self.io_blocks_per_morsel_max.max(blocks);
        if blocks > 0 {
            self.morsels_blocked_for_io += 1;
        }
    }
}

fn min_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ScanStats;

    #[test]
    fn records_one_morsel_from_cumulative_counters() {
        let mut stats = ScanStats {
            plan_polls: 4,
            io_uses: 9,
            planning_time: Duration::from_micros(20),
            ..ScanStats::default()
        };
        stats.begin_morsel_trace(3, 2, 100, 140, 2);

        stats.plan_polls += 1;
        stats.execute_polls += 2;
        stats.io_uses += 2;
        stats.io_requests += 1;
        stats.planning_time += Duration::from_micros(5);
        stats.execution_time += Duration::from_micros(11);
        stats.record_decode(17);
        stats.record_decode_reuse(59);
        stats.finish_morsel_trace(3, 2, vec![17], Duration::from_micros(30));

        let trace = &stats.morsel_traces[0];
        assert_eq!(trace.index, 3);
        assert_eq!(trace.worker, 2);
        assert_eq!((trace.row_start, trace.row_end), (100, 140));
        assert_eq!(trace.selected_rows, 2);
        assert_eq!(trace.output_rows, 2);
        assert_eq!(trace.plan_polls, 1);
        assert_eq!(trace.execute_polls, 2);
        assert_eq!(trace.io_uses, 2);
        assert_eq!(trace.io_requests, 1);
        assert_eq!(trace.decoded_segment_ids, [17]);
        assert_eq!(trace.reused_segment_ids, [59]);
        assert_eq!(trace.planning_time, Duration::from_micros(5));
        assert_eq!(trace.execution_time, Duration::from_micros(11));
        assert_eq!(trace.wall_time, Duration::from_micros(30));
    }
}
