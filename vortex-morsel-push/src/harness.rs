// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The fair-comparison harness.
//!
//! The contract, lifted from the self-paced experiment: the V1 `LayoutReader` is both a row in
//! the matrix and the oracle. Every executor's output is validated against V1's — equal row
//! count and equal ordered content — *before* anything is timed, so a run that is fast because
//! it dropped rows can never be reported as a win.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use futures::TryStreamExt;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::fns::all_non_distinct::all_non_distinct;
use vortex_array::arrays::ChunkedArray;
use vortex_array::dtype::DType;
use vortex_array::expr::Expression;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_io::runtime::single::block_on;
use vortex_io::runtime::tokio::TokioRuntime;
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::LayoutRef;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;
use vortex_session::VortexSession;

use crate::build::build_plan;
use crate::driver::DemandHintDelivery;
use crate::driver::MorselScan;
use crate::driver::morsels;
use crate::io::IoKey;
use crate::nodes::ConjunctMode;
use crate::source::SegmentSourceDriver;
use crate::stats::ScanStats;

/// One query against one fixture.
#[derive(Clone)]
pub struct Query {
    /// A short name for reporting.
    pub name: &'static str,
    /// The projection expression, unbound.
    pub projection: Expression,
    /// The filter expression, unbound.
    pub filter: Option<Expression>,
}

/// The outcome of one executor run.
pub struct RunOutcome {
    /// The batches, in row order.
    pub batches: Vec<ArrayRef>,
    /// Total rows emitted.
    pub rows: usize,
    /// Wall time of the run.
    pub wall: Duration,
    /// Time from the start of the run to the first emitted batch.
    pub time_to_first_batch: Option<Duration>,
    /// Executor counters, where the executor reports them.
    pub stats: Option<ScanStats>,
    /// I/O operations observed by benchmark instrumentation at the runner's measurement layer.
    pub source_io_requests: Option<u64>,
    /// I/O bytes observed by benchmark instrumentation at the runner's measurement layer.
    pub source_io_bytes: Option<u64>,
    /// Submission nowait attempts, hits, misses, and unsupported results at the reader layer.
    pub source_nowait: Option<(u64, u64, u64, u64)>,
    /// Physical-reader slot occupancy observed by benchmark instrumentation.
    pub source_io_occupancy: Option<SourceIoOccupancy>,
}

/// Time-integrated occupancy of the physical reader between its first submission and final
/// completion. Idle time after all reads finish is excluded.
#[derive(Clone, Copy, Debug, Default)]
pub struct SourceIoOccupancy {
    /// Physical concurrency limit used by the reader.
    pub capacity: usize,
    /// Greatest number of physical requests simultaneously in flight.
    pub max_active: usize,
    /// Time from the first physical submission to the final physical completion.
    pub span: Duration,
    /// Time during `span` with every physical slot occupied.
    pub full: Duration,
    /// Time during `span` with some, but not all, physical slots occupied.
    pub underfilled: Duration,
    /// Gaps during `span` with no physical request in flight before a later submission.
    pub idle: Duration,
    /// Underfilled time followed by a later submission, excluding the final drain.
    pub refill_wait: Duration,
    /// Underfilled time after the last submission while its remaining reads drain.
    pub tail_drain: Duration,
    /// Integral of active physical slots over time, in nanoseconds.
    pub active_slot_nanos: u128,
}

/// Run the V1 `LayoutReader` scan path.
pub fn run_v1(
    session: &VortexSession,
    layout: &LayoutRef,
    segments: &Arc<dyn SegmentSource>,
    query: &Query,
) -> VortexResult<RunOutcome> {
    let reader = layout.new_reader(
        "morsel-harness".into(),
        Arc::clone(segments),
        session,
        &Default::default(),
    )?;
    let projection = query.projection.bind(reader.dtype())?;
    let filter = query
        .filter
        .as_ref()
        .map(|expr| expr.bind(reader.dtype()))
        .transpose()?;

    let session = session.clone();
    let start = Instant::now();
    let (batches, first) = block_on(move |handle| {
        let session = session.with_handle(handle);
        async move {
            let stream = ScanBuilder::new(session, reader)
                .with_projection(projection)
                .with_some_filter(filter)
                .with_ordered(true)
                .into_stream()?;
            futures::pin_mut!(stream);

            let mut batches: Vec<ArrayRef> = Vec::new();
            let mut first: Option<Duration> = None;
            while let Some(batch) = stream.try_next().await? {
                if first.is_none() {
                    first = Some(start.elapsed());
                }
                batches.push(batch);
            }
            VortexResult::Ok((batches, first))
        }
    })?;
    let wall = start.elapsed();

    let rows = batches.iter().map(|b| b.len()).sum();
    Ok(RunOutcome {
        batches,
        rows,
        wall,
        time_to_first_batch: first,
        stats: None,
        source_io_requests: None,
        source_io_bytes: None,
        source_nowait: None,
        source_io_occupancy: None,
    })
}

/// Run the V1 `LayoutReader` scan path on a multi-threaded Tokio runtime.
///
/// The single-threaded [`run_v1`] is the apples-to-apples row against a one-thread morsel run;
/// this is the row that gives V1 the same core count the morsel driver gets, which is how it is
/// actually driven under DataFusion.
pub fn run_v1_tokio(
    runtime: &tokio::runtime::Runtime,
    session: &VortexSession,
    layout: &LayoutRef,
    segments: &Arc<dyn SegmentSource>,
    query: &Query,
) -> VortexResult<RunOutcome> {
    run_v1_tokio_with(runtime, session, layout, segments, query, None)
}

/// Run V1 on Tokio with an explicit per-worker split concurrency.
///
/// V1's parallelism has two knobs: the runtime's worker count, and how many splits each worker
/// keeps in flight (`concurrency`, default 4). The product is V1's real concurrent-unit count,
/// which is what to compare against the morsel driver's thread count.
pub fn run_v1_tokio_with(
    runtime: &tokio::runtime::Runtime,
    session: &VortexSession,
    layout: &LayoutRef,
    segments: &Arc<dyn SegmentSource>,
    query: &Query,
    concurrency: Option<usize>,
) -> VortexResult<RunOutcome> {
    let reader = layout.new_reader(
        "morsel-harness".into(),
        Arc::clone(segments),
        session,
        &Default::default(),
    )?;
    let projection = query.projection.bind(reader.dtype())?;
    let filter = query
        .filter
        .as_ref()
        .map(|expr| expr.bind(reader.dtype()))
        .transpose()?;

    let session = session.clone();
    let start = Instant::now();
    let (batches, first) = runtime.block_on(async move {
        let session = session.with_handle(TokioRuntime::current());
        let mut builder = ScanBuilder::new(session, reader)
            .with_projection(projection)
            .with_some_filter(filter)
            .with_ordered(true);
        if let Some(concurrency) = concurrency {
            builder = builder.with_concurrency(concurrency);
        }
        let stream = builder.into_stream()?;
        futures::pin_mut!(stream);

        let mut batches: Vec<ArrayRef> = Vec::new();
        let mut first: Option<Duration> = None;
        while let Some(batch) = stream.try_next().await? {
            if first.is_none() {
                first = Some(start.elapsed());
            }
            batches.push(batch);
        }
        VortexResult::Ok((batches, first))
    })?;
    let wall = start.elapsed();

    let rows = batches.iter().map(|b| b.len()).sum();
    Ok(RunOutcome {
        batches,
        rows,
        wall,
        time_to_first_batch: first,
        stats: None,
        source_io_requests: None,
        source_io_bytes: None,
        source_nowait: None,
        source_io_occupancy: None,
    })
}

/// How to configure one morsel-executor run.
#[derive(Clone, Copy, Debug)]
pub struct MorselConfig {
    /// Driving threads.
    pub threads: usize,
    /// Morsel coalescing target; zero means "one morsel per natural split", matching V1.
    pub morsel_rows: u64,
    /// Conjunct evaluation policy.
    pub mode: ConjunctMode,
    /// Whether the leased shared decoded cells are enabled.
    pub share_decodes: bool,
    /// Future morsels admitted to filtered background I/O beyond active workers.
    pub lookahead_morsels: usize,
    /// Optional demand-hint delivery policy.
    pub demand_hints: DemandHintDelivery,
}

impl Default for MorselConfig {
    fn default() -> Self {
        Self {
            threads: 1,
            morsel_rows: 0,
            mode: ConjunctMode::Cascade,
            share_decodes: true,
            lookahead_morsels: 0,
            demand_hints: DemandHintDelivery::Immediate,
        }
    }
}

/// Run the morsel executor with worker lifecycle excluded from the reported wall time.
///
/// This matches the V1 Tokio rows, whose runtime workers are also created outside their timed
/// interval. Plan construction and morsel cutting remain outside timing for the same reason V1's
/// reader construction and expression binding do; scan-specific preparation and execution remain
/// inside timing.
pub fn run_morsel(
    session: &VortexSession,
    layout: &LayoutRef,
    segments: &Arc<dyn SegmentSource>,
    query: &Query,
    config: MorselConfig,
) -> VortexResult<RunOutcome> {
    let capture_path = std::env::var_os("VORTEX_MORSEL_IO_ORACLE_CAPTURE").map(PathBuf::from);
    let replay_path = std::env::var_os("VORTEX_MORSEL_IO_ORACLE_REPLAY").map(PathBuf::from);
    let replay_order = replay_path
        .as_deref()
        .map(load_io_oracle)
        .transpose()?
        .unwrap_or_default();
    let round_robin = std::env::var("VORTEX_MORSEL_IO_ROUND_ROBIN").is_ok_and(|value| value == "1");
    let io_depth = std::env::var("TPCH_IO_DEPTH")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|err| vortex_err!("invalid TPCH_IO_DEPTH: {err}"))
        })
        .transpose()?
        .unwrap_or(16)
        .max(1);
    let oracle_enabled = capture_path.is_some() || replay_path.is_some();
    let plan = Arc::new(build_plan(
        layout,
        &query.projection,
        query.filter.as_ref(),
        config.mode,
    )?);
    let cut = morsels(&plan, config.morsel_rows);
    let scan = MorselScan::new(plan, session.clone())
        .with_threads(config.threads)
        .with_morsels(cut)
        .with_share_decodes(config.share_decodes)
        .with_lookahead_morsels(config.lookahead_morsels)
        .with_demand_hints(config.demand_hints)
        .with_io_round_robin(round_robin);
    let scan = if oracle_enabled {
        scan.with_io_oracle(replay_order)
    } else {
        scan
    };
    let submission_nowait = std::env::var("TPCH_HOT_INLINE_MAX_BYTES").is_ok();
    let scan = SegmentSourceDriver::new(Arc::clone(segments))
        .with_background_window(io_depth)
        .with_submission_nowait(submission_nowait)
        .connect_on_thread(scan)?;

    let (batches, stats, wall) = scan.run_timed()?;
    if let Some(path) = capture_path {
        let snapshot = scan
            .io_oracle_snapshot()
            .ok_or_else(|| vortex_err!("I/O scheduling oracle was not enabled"))?;
        write_io_oracle(&path, &snapshot.learned_order)?;
    }

    let rows = batches.iter().map(|b| b.len()).sum();
    Ok(RunOutcome {
        rows,
        time_to_first_batch: stats.time_to_first_batch,
        batches,
        wall,
        stats: Some(stats),
        source_io_requests: None,
        source_io_bytes: None,
        source_nowait: None,
        source_io_occupancy: None,
    })
}

fn load_io_oracle(path: &Path) -> VortexResult<Vec<IoKey>> {
    let contents = std::fs::read_to_string(path)?;
    contents
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let line = line.trim();
            (!line.is_empty() && !line.starts_with('#')).then_some((index, line))
        })
        .map(|(index, line)| {
            let id = line.parse::<u32>().map_err(|err| {
                vortex_err!(
                    "invalid segment ID on line {} of {}: {err}",
                    index + 1,
                    path.display()
                )
            })?;
            Ok(IoKey::Segment(SegmentId::from(id)))
        })
        .collect()
}

fn write_io_oracle(path: &Path, order: &[IoKey]) -> VortexResult<()> {
    let mut contents = String::from("# vortex-morsel-push deadline-miss order v2\n");
    for key in order {
        match key {
            IoKey::Segment(id) => {
                contents.push_str(&(**id).to_string());
                contents.push('\n');
            }
        }
    }
    std::fs::write(path, contents)?;
    Ok(())
}

/// Assert that two runs produced the same rows in the same order.
///
/// Batching may differ — the executors cut morsels differently — so the batches are concatenated
/// before comparison. Content equality is an O(rows) vectorised comparison, not a scalar walk.
pub fn assert_same_rows(
    session: &VortexSession,
    dtype: &DType,
    left: &RunOutcome,
    right: &RunOutcome,
) -> VortexResult<()> {
    if left.rows != right.rows {
        vortex_bail!(
            "row count mismatch: {} rows vs {} rows",
            left.rows,
            right.rows
        );
    }
    if left.rows == 0 {
        return Ok(());
    }

    let left = concat(&left.batches, dtype)?;
    let right = concat(&right.batches, dtype)?;
    let mut ctx = session.create_execution_ctx();
    if !all_non_distinct(&left, &right, &mut ctx)? {
        vortex_bail!("ordered content mismatch between executors");
    }
    Ok(())
}

/// Concatenate a run's batches into one array.
pub fn concat(batches: &[ArrayRef], dtype: &DType) -> VortexResult<ArrayRef> {
    match batches.len() {
        0 => Ok(vortex_array::Canonical::empty(dtype).into_array()),
        1 => Ok(batches[0].clone()),
        _ => Ok(ChunkedArray::try_new(batches.to_vec(), dtype.clone())?.into_array()),
    }
}
