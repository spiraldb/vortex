// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! What a V2 scan's stages did, for the engine's metrics.

use vortex_metrics::Counter;
use vortex_metrics::DefaultMetricsRegistry;
use vortex_metrics::Label;
use vortex_metrics::MetricBuilder;
use vortex_metrics::MetricsRegistry;
use vortex_metrics::Timer;

/// The metrics of one prepared scan, updated by its planners and morsels as they run. Every
/// name starts with `vortex.scan.`; a duration is per stage run, a row count is summed.
pub struct ScanMetrics {
    /// Time from a prune planner's first compute to its last, once per partition.
    pub prune_duration: Timer,
    /// Rows pruning looked at, and the rows it kept.
    pub prune_rows_in: Counter,
    pub prune_rows_kept: Counter,
    /// Requests the prune planner speculated while its zone maps were read, and requests the
    /// read-ahead planner started or announced for the chunks ahead.
    pub speculated_requests: Counter,
    pub read_ahead_requests: Counter,
    /// Chunks planned, and chunks skipped because nothing was selected or kept of them.
    pub chunks: Counter,
    pub chunks_skipped: Counter,
    /// Time from a filter planner's first compute to the projection it emits, per chunk.
    pub filter_duration: Timer,
    /// Conjunct evaluations, with the rows each one saw and the rows that passed it.
    pub filter_conjuncts: Counter,
    pub filter_rows_in: Counter,
    pub filter_rows_out: Counter,
    /// Time from a projection morsel's first compute to its batch, and from its reads going
    /// out to its first piece, per morsel.
    pub projection_duration: Timer,
    pub projection_wait: Timer,
    /// Rows projected, and batches handed to the engine.
    pub projection_rows: Counter,
    pub batches: Counter,
}

impl ScanMetrics {
    /// Registers the scan's metrics with `registry` under `labels`.
    pub fn new(registry: &dyn MetricsRegistry, labels: &[Label]) -> Self {
        let counter = |name: &'static str| {
            MetricBuilder::new(registry)
                .add_labels(labels.iter().cloned())
                .counter(name)
        };
        let timer = |name: &'static str| {
            MetricBuilder::new(registry)
                .add_labels(labels.iter().cloned())
                .timer(name)
        };
        Self {
            prune_duration: timer("vortex.scan.prune.duration"),
            prune_rows_in: counter("vortex.scan.prune.rows_in"),
            prune_rows_kept: counter("vortex.scan.prune.rows_kept"),
            speculated_requests: counter("vortex.scan.speculated.requests"),
            read_ahead_requests: counter("vortex.scan.read_ahead.requests"),
            chunks: counter("vortex.scan.chunks"),
            chunks_skipped: counter("vortex.scan.chunks.skipped"),
            filter_duration: timer("vortex.scan.filter.duration"),
            filter_conjuncts: counter("vortex.scan.filter.conjuncts"),
            filter_rows_in: counter("vortex.scan.filter.rows_in"),
            filter_rows_out: counter("vortex.scan.filter.rows_out"),
            projection_duration: timer("vortex.scan.projection.duration"),
            projection_wait: timer("vortex.scan.projection.wait"),
            projection_rows: counter("vortex.scan.projection.rows"),
            batches: counter("vortex.scan.batches"),
        }
    }

    /// Metrics nobody reads, for a scan run without a registry.
    pub fn unregistered() -> Self {
        Self::new(&DefaultMetricsRegistry::default(), &[])
    }
}
