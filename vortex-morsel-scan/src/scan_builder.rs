// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::StreamExt;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use parking_lot::Mutex;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::dtype::DType;
use vortex_array::expr::BoundExpression;
use vortex_array::stream::ArrayStream;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::LayoutRef;
use vortex_layout::segments::SegmentSource;
use vortex_metrics::Counter;
use vortex_metrics::Label;
use vortex_metrics::MetricBuilder;
use vortex_metrics::MetricsRegistry;
use vortex_morsel_push::PushMorselScanExecutor;
use vortex_morsel_push::ScanStats;
use vortex_scan::selection::Selection;
use vortex_session::VortexSession;
use vortex_utils::parallelism::get_available_parallelism;

use crate::ScanBackend;
use crate::ScanExecutorOptions;

type OutputTask<A> = BoxFuture<'static, VortexResult<Option<A>>>;
type MetricsCompletion = BoxFuture<'static, VortexResult<()>>;
type InstrumentedBuild<A> = (Vec<OutputTask<A>>, Option<MetricsCompletion>);

const SCAN_COUNTER_NAMES: [&str; 31] = [
    "morsel_scan.plan.morsels_in_row_range",
    "morsel_scan.selection.morsels_remaining",
    "morsel_scan.selection.ranges_remaining",
    "morsel_scan.executor_limit.morsels_remaining",
    "morsel_scan.executor_limit.ranges_remaining",
    "morsel_scan.pruning.morsels_remaining",
    "morsel_scan.pruning.ranges_remaining",
    "morsel_scan.executor.ranges_driven",
    "morsel_scan.executor.empty_ranges",
    "morsel_scan.executor.root_batches",
    "morsel_scan.output.batches_before_map",
    "morsel_scan.output.rows_before_map",
    "morsel_scan.io.uses",
    "morsel_scan.io.requests",
    "morsel_scan.io.batches",
    "morsel_scan.io.bytes",
    "morsel_scan.io.waits",
    "morsel_scan.io.cell_hits",
    "morsel_scan.io.cells_registered",
    "morsel_scan.io.cancellations",
    "morsel_scan.io.nowait_attempts",
    "morsel_scan.io.nowait_hits",
    "morsel_scan.io.nowait_misses",
    "morsel_scan.io.nowait_unsupported",
    "morsel_scan.blocked.io_suspensions",
    "morsel_scan.blocked.morsels_for_io",
    "morsel_scan.blocked.output_credit",
    "morsel_scan.io.lookahead_refills",
    "morsel_scan.decode.calls",
    "morsel_scan.decode.reuses",
    "morsel_scan.io.wait_nanoseconds",
];

/// Builder for push scans over a raw layout and segment source.
///
/// Unlike [`vortex_layout::scan::scan_builder::ScanBuilder`], this builder never constructs or
/// accepts a layout reader. Layout planning and predicate execution are owned entirely by the
/// selected morsel implementation.
pub struct MorselScanBuilder<A> {
    session: VortexSession,
    executor: PushMorselScanExecutor,
    projection: BoundExpression,
    filter: Option<BoundExpression>,
    ordered: bool,
    row_range: Option<Range<u64>>,
    selection: Selection,
    concurrency: usize,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    limit: Option<u64>,
    row_offset: u64,
    scan_stats_available: bool,
    metrics: Option<Arc<MorselScanMetrics>>,
}

impl MorselScanBuilder<ArrayRef> {
    /// Create a dedicated push builder over raw file layout state.
    pub fn new(
        session: VortexSession,
        backend: ScanBackend,
        layout: LayoutRef,
        segments: Arc<dyn SegmentSource>,
        options: &ScanExecutorOptions,
    ) -> VortexResult<Self> {
        let projection = BoundExpression::new_root(layout.dtype().clone());
        let scan_stats_available = options.external_driver.is_none();
        let executor = match backend {
            ScanBackend::V1 => {
                vortex_bail!("MorselScanBuilder only supports the push backend")
            }
            ScanBackend::Push | ScanBackend::PushFrontier => {
                let mut executor = PushMorselScanExecutor::new(layout, segments)
                    .with_threads(options.threads)
                    .with_frontier_io(backend == ScanBackend::PushFrontier);
                if let Some(driver) = &options.external_driver {
                    executor = executor.with_external_threads(Arc::clone(driver));
                }
                executor
            }
        };
        Ok(Self {
            session,
            executor,
            projection,
            filter: None,
            ordered: true,
            row_range: None,
            selection: Selection::All,
            concurrency: 4,
            map_fn: Arc::new(Ok),
            limit: None,
            row_offset: 0,
            scan_stats_available,
            metrics: None,
        })
    }

    /// Return an array stream for this scan.
    pub fn into_array_stream(self) -> VortexResult<impl ArrayStream + Send + 'static> {
        let dtype = self.dtype();
        Ok(ArrayStreamAdapter::new(dtype, self.into_stream()?))
    }
}

impl<A: 'static + Send> MorselScanBuilder<A> {
    /// Set the bound filter expression.
    pub fn with_filter(mut self, filter: BoundExpression) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set or clear the bound filter expression.
    pub fn with_some_filter(mut self, filter: Option<BoundExpression>) -> Self {
        self.filter = filter;
        self
    }

    /// Set the bound output projection.
    pub fn with_projection(mut self, projection: BoundExpression) -> Self {
        self.projection = projection;
        self
    }

    /// Configure whether output futures are consumed in file order.
    pub fn with_ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    /// Restrict scanning to a contiguous root row range.
    pub fn with_row_range(mut self, row_range: Range<u64>) -> Self {
        self.row_range = Some(row_range);
        self
    }

    /// Apply a row selection inside the configured row range.
    pub fn with_selection(mut self, selection: Selection) -> Self {
        self.selection = selection;
        self
    }

    /// Configure the number of output futures driven concurrently per runtime worker.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0);
        self.concurrency = concurrency;
        self
    }

    /// Publish final scan statistics through the metrics registry.
    ///
    /// This applies to internally driven scans. Externally driven scans currently execute one run
    /// per output future and do not expose one scan-wide completion point.
    pub fn with_metrics_registry(self, metrics: Arc<dyn MetricsRegistry>) -> Self {
        self.with_metrics_registry_and_labels(metrics, Vec::new())
    }

    /// Publish final scan statistics with labels shared by every metric.
    ///
    /// Metrics are registered only after the executor and every post-limit mapped output task
    /// complete successfully. Failed or cancelled scans do not expose partial values. Selection,
    /// executor-limit, and pruning metrics describe executor phases; output rows and batches count
    /// arrays presented to the generic map callback after this builder applies its final limit.
    /// Every published value is an additive counter, so scans with the same partition label can be
    /// summed safely. Per-scan peaks, final gauges, and time-to-first-batch are omitted until the
    /// shared registry can express max/min aggregation. Externally driven scans do not currently
    /// publish these scan-wide metrics.
    pub fn with_metrics_registry_and_labels(
        self,
        metrics: Arc<dyn MetricsRegistry>,
        labels: Vec<Label>,
    ) -> Self {
        self.with_scan_metrics(Arc::new(MorselScanMetrics::new(metrics, labels)))
    }

    /// Publish through a reusable scan-metrics sink.
    ///
    /// Integrations that scan multiple files or ranges in one partition should share this sink so
    /// its lazily registered counter handles are reused across every completed scan.
    pub fn with_scan_metrics(mut self, metrics: Arc<MorselScanMetrics>) -> Self {
        if self.scan_stats_available {
            self.metrics = Some(metrics);
        }
        self
    }

    /// Set the maximum number of output rows.
    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the root offset used by row-index expressions.
    pub fn with_row_offset(mut self, row_offset: u64) -> Self {
        self.row_offset = row_offset;
        self
    }

    /// Return the projected output dtype.
    pub fn dtype(&self) -> DType {
        self.projection.dtype().clone()
    }

    /// Compute full-file natural boundaries using the selected morsel planner.
    pub fn full_file_splits(&self) -> VortexResult<Vec<u64>> {
        self.executor
            .full_file_splits(&self.projection, self.filter.as_ref())
    }

    /// Map every non-empty output array.
    ///
    /// The callback may return any type and does not promise to preserve row cardinality. Scan
    /// output row and batch metrics therefore describe the arrays before this callback.
    pub fn map<B: 'static + Send>(
        self,
        map_fn: impl Fn(A) -> VortexResult<B> + 'static + Send + Sync,
    ) -> MorselScanBuilder<B> {
        let old_map_fn = self.map_fn;
        MorselScanBuilder {
            session: self.session,
            executor: self.executor,
            projection: self.projection,
            filter: self.filter,
            ordered: self.ordered,
            row_range: self.row_range,
            selection: self.selection,
            concurrency: self.concurrency,
            map_fn: Arc::new(move |array| old_map_fn(array).and_then(&map_fn)),
            limit: self.limit,
            row_offset: self.row_offset,
            scan_stats_available: self.scan_stats_available,
            metrics: self.metrics,
        }
    }

    /// Build one independently awaitable task per output unit.
    ///
    /// Scan-wide metrics require observing successful stream completion and are therefore emitted
    /// only by [`Self::into_stream`]. This method never appends a hidden completion task.
    pub fn build(mut self) -> VortexResult<Vec<BoxFuture<'static, VortexResult<Option<A>>>>> {
        self.metrics = None;
        let (tasks, completion) = self.build_tasks()?;
        debug_assert!(completion.is_none());
        Ok(tasks)
    }

    fn build_tasks(self) -> VortexResult<InstrumentedBuild<A>> {
        if self.limit == Some(0) && self.metrics.is_none() {
            return Ok((Vec::new(), None));
        }
        let limit = self.limit;
        let map_fn = Arc::clone(&self.map_fn);
        let (tasks, stats_completion) = match &self.metrics {
            Some(_) => {
                let (tasks, completion) = self.executor.build_with_stats(
                    self.session,
                    self.projection,
                    self.filter,
                    self.row_range,
                    self.selection,
                    limit,
                    self.row_offset,
                )?;
                (tasks, Some(completion))
            }
            None => (
                self.executor.build(
                    self.session,
                    self.projection,
                    self.filter,
                    self.row_range,
                    self.selection,
                    limit,
                    self.row_offset,
                )?,
                None,
            ),
        };
        let tasks = match limit {
            Some(limit) => limit_tasks(tasks, limit),
            None => tasks,
        };
        match (self.metrics, stats_completion) {
            (Some(publisher), Some(completion)) => {
                let (tasks, completion) =
                    instrument_output_tasks(tasks, map_fn, completion, publisher);
                Ok((tasks, Some(completion)))
            }
            (None, None) => Ok((map_output_tasks(tasks, map_fn), None)),
            _ => unreachable!("metrics publisher and stats completion are configured together"),
        }
    }

    /// Return a runtime-driven stream of non-empty scan outputs.
    pub fn into_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        let ordered = self.ordered;
        let concurrency = self.concurrency * get_available_parallelism().unwrap_or(1);
        let handle = self.session.handle();
        let (tasks, completion) = self.build_tasks()?;
        let stream = futures::stream::iter(tasks).map(move |task| handle.spawn(task));
        let stream = if ordered {
            stream.buffered(concurrency).boxed()
        } else {
            stream.buffer_unordered(concurrency).boxed()
        };
        let stream = stream
            .filter_map(|result| async move { result.transpose() })
            .boxed();
        let Some(completion) = completion else {
            return Ok(stream);
        };
        let completion = futures::stream::once(completion)
            .filter_map(|result| async move { result.err().map(Err) });
        Ok(stream.chain(completion).boxed())
    }
}

fn map_output_tasks<A: 'static + Send>(
    tasks: Vec<BoxFuture<'static, VortexResult<Option<ArrayRef>>>>,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
) -> Vec<BoxFuture<'static, VortexResult<Option<A>>>> {
    tasks
        .into_iter()
        .map(move |task| {
            let map_fn = Arc::clone(&map_fn);
            Box::pin(async move { task.await?.map(|array| map_fn(array)).transpose() })
                as BoxFuture<'static, VortexResult<Option<A>>>
        })
        .collect()
}

struct OutputAccounting {
    remaining: AtomicUsize,
    rows: AtomicU64,
    batches: AtomicU64,
    failed: AtomicBool,
    done: Mutex<Option<oneshot::Sender<()>>>,
}

enum OutputCompletion {
    Batch(u64),
    Empty,
    Failed,
}

impl OutputAccounting {
    fn new(task_count: usize) -> (Arc<Self>, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        let accounting = Arc::new(Self {
            remaining: AtomicUsize::new(task_count),
            rows: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            failed: AtomicBool::new(false),
            done: Mutex::new(Some(sender)),
        });
        if task_count == 0 {
            accounting.signal_done();
        }
        (accounting, receiver)
    }

    fn finish(&self, completion: OutputCompletion) {
        match completion {
            OutputCompletion::Batch(rows) => {
                self.rows.fetch_add(rows, Ordering::Relaxed);
                self.batches.fetch_add(1, Ordering::Relaxed);
            }
            OutputCompletion::Empty => {}
            OutputCompletion::Failed => {
                self.failed.store(true, Ordering::Relaxed);
            }
        }
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.signal_done();
        }
    }

    fn signal_done(&self) {
        let mut done = self.done.lock();
        if let Some(sender) = done.take() {
            let _ = sender.send(());
        }
    }
}

struct OutputAccountingGuard {
    accounting: Arc<OutputAccounting>,
    finished: bool,
}

impl OutputAccountingGuard {
    fn success(mut self, output_rows: Option<u64>) {
        self.accounting.finish(match output_rows {
            Some(rows) => OutputCompletion::Batch(rows),
            None => OutputCompletion::Empty,
        });
        self.finished = true;
    }
}

impl Drop for OutputAccountingGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.accounting.finish(OutputCompletion::Failed);
        }
    }
}

fn instrument_output_tasks<A: 'static + Send>(
    tasks: Vec<BoxFuture<'static, VortexResult<Option<ArrayRef>>>>,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    stats_completion: BoxFuture<'static, VortexResult<ScanStats>>,
    publisher: Arc<MorselScanMetrics>,
) -> (Vec<OutputTask<A>>, MetricsCompletion) {
    let (accounting, done) = OutputAccounting::new(tasks.len());
    let outputs = tasks
        .into_iter()
        .map(|task| {
            let accounting = Arc::clone(&accounting);
            let map_fn = Arc::clone(&map_fn);
            Box::pin(async move {
                let guard = OutputAccountingGuard {
                    accounting,
                    finished: false,
                };
                let output = task.await?;
                let rows = output
                    .as_ref()
                    .map(|array| u64::try_from(array.len()).unwrap_or(u64::MAX));
                let output = output.map(|array| map_fn(array)).transpose()?;
                guard.success(rows);
                Ok(output)
            }) as BoxFuture<'static, VortexResult<Option<A>>>
        })
        .collect::<Vec<_>>();
    let completion = Box::pin(async move {
        done.await
            .map_err(|_| vortex_error::vortex_err!("scan output accounting stopped"))?;
        if accounting.failed.load(Ordering::Acquire) {
            return Ok(());
        }
        let stats = stats_completion.await?;
        publisher.publish(
            &stats,
            accounting.batches.load(Ordering::Acquire),
            accounting.rows.load(Ordering::Acquire),
        );
        Ok(())
    });
    (outputs, completion)
}

/// Reusable additive scan-metrics sink.
///
/// Construction does not register metrics. The first successfully completed stream lazily
/// registers one fixed counter set, after which every scan only fetches those handles and adds its
/// final values. Sharing one sink across an execution partition therefore bounds registry objects
/// independently of the number of files or ranges scanned.
pub struct MorselScanMetrics {
    registry: Arc<dyn MetricsRegistry>,
    labels: Arc<[Label]>,
    counters: OnceLock<Box<[Counter]>>,
}

impl MorselScanMetrics {
    /// Create a lazily registered counter set with labels shared by every metric.
    pub fn new(registry: Arc<dyn MetricsRegistry>, labels: Vec<Label>) -> Self {
        Self {
            registry,
            labels: labels.into(),
            counters: OnceLock::new(),
        }
    }

    fn publish(&self, stats: &ScanStats, output_batches: u64, output_rows: u64) {
        let counters = self.counters.get_or_init(|| {
            SCAN_COUNTER_NAMES
                .iter()
                .map(|name| {
                    MetricBuilder::new(self.registry.as_ref())
                        .add_labels(self.labels.iter().cloned())
                        .counter(*name)
                })
                .collect()
        });
        for (counter, value) in
            counters
                .iter()
                .zip(scan_counter_values(stats, output_batches, output_rows))
        {
            counter.add(value);
        }
    }
}

fn scan_counter_values(stats: &ScanStats, output_batches: u64, output_rows: u64) -> [u64; 31] {
    [
        stats.morsels_in_row_range,
        stats.morsels_after_selection,
        stats.ranges_after_selection,
        stats.morsels_after_limit,
        stats.ranges_after_limit,
        stats.morsels_after_pruning,
        stats.ranges_after_pruning,
        stats.morsels,
        stats.morsels_empty,
        stats.push_root_batches,
        output_batches,
        output_rows,
        stats.io_uses,
        stats.io_requests,
        stats.io_batches,
        stats.io_bytes,
        stats.io_waits,
        stats.io_cell_hits,
        stats.io_registered,
        stats.io_cancellations,
        stats.nowait_attempts,
        stats.nowait_hits,
        stats.nowait_misses,
        stats.nowait_unsupported,
        stats.execute_io_blocks,
        stats.morsels_blocked_for_io,
        stats.output_credit_blocks,
        stats.lookahead_refills,
        stats.decodes,
        stats.decode_reuses,
        u64::try_from(stats.io_wait_time.as_nanos()).unwrap_or(u64::MAX),
    ]
}

fn limit_tasks(
    tasks: Vec<BoxFuture<'static, VortexResult<Option<ArrayRef>>>>,
    limit: u64,
) -> Vec<BoxFuture<'static, VortexResult<Option<ArrayRef>>>> {
    vec![Box::pin(async move {
        let mut remaining = limit;
        let mut batches = Vec::new();
        for task in tasks {
            if remaining == 0 {
                break;
            }
            let Some(batch) = task.await? else {
                continue;
            };
            let take = batch
                .len()
                .min(usize::try_from(remaining).unwrap_or(usize::MAX));
            remaining -= take as u64;
            if take == batch.len() {
                batches.push(batch);
            } else {
                batches.push(batch.slice(0..take)?);
            }
        }
        match batches.len() {
            0 => Ok(None),
            1 => Ok(batches.pop()),
            _ => {
                let dtype = batches[0].dtype().clone();
                ChunkedArray::try_new(batches, dtype).map(|array| Some(array.into_array()))
            }
        }
    })]
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use futures::executor::block_on;
    use futures::future;
    use vortex_array::IntoArray;
    use vortex_array::array_session;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_error::vortex_err;
    use vortex_io::runtime::single::block_on as vortex_block_on;
    use vortex_io::session::RuntimeSession;
    use vortex_io::session::RuntimeSessionExt;
    use vortex_layout::session::LayoutSession;
    use vortex_metrics::DefaultMetricsRegistry;
    use vortex_metrics::MetricValue;
    use vortex_morsel_push::fixtures::Column;
    use vortex_morsel_push::fixtures::write_fixture;

    use super::*;

    #[test]
    fn limit_spans_ordered_output_tasks() -> VortexResult<()> {
        let tasks = [[1i32, 2], [3, 4]]
            .into_iter()
            .map(|values| {
                Box::pin(future::ready(Ok(Some(
                    PrimitiveArray::from_iter(values).into_array(),
                )))) as BoxFuture<'static, VortexResult<Option<ArrayRef>>>
            })
            .collect();

        let mut limited = limit_tasks(tasks, 3);
        let task = limited
            .pop()
            .ok_or_else(|| vortex_err!("limit did not return an output task"))?;
        let output = block_on(task)?.ok_or_else(|| vortex_err!("limit returned no rows"))?;
        assert_eq!(output.len(), 3);
        Ok(())
    }

    fn output_task(
        values: impl IntoIterator<Item = i32>,
    ) -> BoxFuture<'static, VortexResult<Option<ArrayRef>>> {
        Box::pin(future::ready(Ok(Some(
            PrimitiveArray::from_iter(values).into_array(),
        ))))
    }

    fn instrumented_stream(
        tasks: Vec<BoxFuture<'static, VortexResult<Option<ArrayRef>>>>,
        stats: ScanStats,
        registry: Arc<dyn MetricsRegistry>,
    ) -> BoxStream<'static, VortexResult<ArrayRef>> {
        instrumented_stream_with_metrics(
            tasks,
            stats,
            Arc::new(MorselScanMetrics::new(
                registry,
                vec![Label::new("partition", "3")],
            )),
        )
    }

    fn instrumented_stream_with_metrics(
        tasks: Vec<BoxFuture<'static, VortexResult<Option<ArrayRef>>>>,
        stats: ScanStats,
        metrics: Arc<MorselScanMetrics>,
    ) -> BoxStream<'static, VortexResult<ArrayRef>> {
        let (tasks, completion) = instrument_output_tasks(
            tasks,
            Arc::new(Ok),
            Box::pin(future::ready(Ok(stats))),
            metrics,
        );
        let outputs = futures::stream::iter(tasks)
            .buffered(4)
            .filter_map(|result| future::ready(result.transpose()))
            .boxed();
        let completion = futures::stream::once(completion)
            .filter_map(|result| future::ready(result.err().map(Err)));
        outputs.chain(completion).boxed()
    }

    fn counter(registry: &dyn MetricsRegistry, name: &str) -> VortexResult<u64> {
        registry
            .snapshot()
            .into_iter()
            .find_map(|metric| {
                (metric.name().as_ref() == name).then(|| match metric.value() {
                    MetricValue::Counter(counter) => Some(counter.value()),
                    _ => None,
                })?
            })
            .ok_or_else(|| vortex_err!("missing counter {name}"))
    }

    #[test]
    fn actual_scan_publishes_metrics_before_stream_eof_is_observed() -> VortexResult<()> {
        let session = array_session()
            .with::<LayoutSession>()
            .with::<RuntimeSession>();
        vortex_block_on(|handle| async move {
            let session = session.with_handle(handle);
            let fixture = write_fixture(
                vec![Column::new(
                    "a",
                    vec![PrimitiveArray::from_iter([1i32, 2, 3]).into_array()],
                )],
                &session,
            )
            .await?;
            let layout = fixture.layout;
            let segments = fixture.segments;
            let registry: Arc<dyn MetricsRegistry> = Arc::new(DefaultMetricsRegistry::default());
            let mut stream = MorselScanBuilder::new(
                session.clone(),
                ScanBackend::Push,
                Arc::clone(&layout),
                Arc::clone(&segments),
                &ScanExecutorOptions::default().with_threads(1),
            )?
            .with_metrics_registry_and_labels(
                Arc::clone(&registry),
                vec![Label::new("partition", "0")],
            )
            .into_stream()?;

            let batch = stream
                .next()
                .await
                .ok_or_else(|| vortex_err!("actual scan produced no output"))??;
            assert_eq!(batch.len(), 3);
            assert!(registry.snapshot().is_empty());
            assert!(stream.next().await.is_none());
            assert_eq!(
                counter(registry.as_ref(), "morsel_scan.output.rows_before_map")?,
                3
            );

            let zero_limit_registry: Arc<dyn MetricsRegistry> =
                Arc::new(DefaultMetricsRegistry::default());
            let mut zero_limit_stream = MorselScanBuilder::new(
                session,
                ScanBackend::Push,
                layout,
                segments,
                &ScanExecutorOptions::default().with_threads(1),
            )?
            .with_limit(0)
            .with_metrics_registry(Arc::clone(&zero_limit_registry))
            .into_stream()?;
            assert!(zero_limit_stream.next().await.is_none());
            assert_eq!(
                counter(
                    zero_limit_registry.as_ref(),
                    "morsel_scan.plan.morsels_in_row_range"
                )?,
                1
            );
            assert_eq!(
                counter(
                    zero_limit_registry.as_ref(),
                    "morsel_scan.selection.morsels_remaining"
                )?,
                1
            );
            assert_eq!(
                counter(
                    zero_limit_registry.as_ref(),
                    "morsel_scan.executor_limit.morsels_remaining"
                )?,
                0
            );
            Ok(())
        })
    }

    #[test]
    fn public_build_returns_only_output_tasks_and_never_waits_for_metrics() -> VortexResult<()> {
        let session = array_session()
            .with::<LayoutSession>()
            .with::<RuntimeSession>();
        vortex_block_on(|handle| async move {
            let session = session.with_handle(handle);
            let fixture = write_fixture(
                vec![Column::new(
                    "a",
                    vec![PrimitiveArray::from_iter([1i32, 2, 3]).into_array()],
                )],
                &session,
            )
            .await?;
            let registry: Arc<dyn MetricsRegistry> = Arc::new(DefaultMetricsRegistry::default());
            let tasks = MorselScanBuilder::new(
                session,
                ScanBackend::Push,
                fixture.layout,
                fixture.segments,
                &ScanExecutorOptions::default().with_threads(1),
            )?
            .with_metrics_registry(Arc::clone(&registry))
            .build()?;
            assert_eq!(tasks.len(), 1);
            let output = future::try_join_all(tasks).await?;
            assert_eq!(output.iter().flatten().map(ArrayRef::len).sum::<usize>(), 3);
            assert!(registry.snapshot().is_empty());
            Ok(())
        })
    }

    #[test]
    fn preselection_empty_metrics_are_published_before_stream_eof() -> VortexResult<()> {
        let registry: Arc<dyn MetricsRegistry> = Arc::new(DefaultMetricsRegistry::default());
        let mut stream =
            instrumented_stream(Vec::new(), ScanStats::default(), Arc::clone(&registry));
        assert!(registry.snapshot().is_empty());
        assert!(block_on(stream.next()).is_none());
        assert_eq!(
            counter(registry.as_ref(), "morsel_scan.output.rows_before_map")?,
            0
        );
        let metric_count = registry.snapshot().len();
        assert!(block_on(stream.next()).is_none());
        assert_eq!(registry.snapshot().len(), metric_count);
        Ok(())
    }

    #[test]
    fn all_pruned_metrics_are_published_before_stream_eof() -> VortexResult<()> {
        let registry: Arc<dyn MetricsRegistry> = Arc::new(DefaultMetricsRegistry::default());
        let tasks =
            vec![Box::pin(future::ready(Ok(None)))
                as BoxFuture<'static, VortexResult<Option<ArrayRef>>>];
        let stats = ScanStats {
            morsels_in_row_range: 1,
            morsels_after_selection: 1,
            ranges_after_selection: 1,
            morsels_after_limit: 1,
            ranges_after_limit: 1,
            ..ScanStats::default()
        };
        let mut stream = instrumented_stream(tasks, stats, Arc::clone(&registry));
        assert!(registry.snapshot().is_empty());
        assert!(block_on(stream.next()).is_none());
        assert_eq!(
            counter(registry.as_ref(), "morsel_scan.pruning.morsels_remaining")?,
            0
        );
        Ok(())
    }

    #[test]
    fn failed_stream_does_not_publish_partial_metrics() -> VortexResult<()> {
        let registry: Arc<dyn MetricsRegistry> = Arc::new(DefaultMetricsRegistry::default());
        let tasks = vec![Box::pin(future::ready(Err(vortex_err!("test failure"))))
            as BoxFuture<'static, VortexResult<Option<ArrayRef>>>];
        let mut stream = instrumented_stream(tasks, ScanStats::default(), Arc::clone(&registry));
        assert!(block_on(stream.next()).is_some_and(|result| result.is_err()));
        assert!(block_on(stream.next()).is_none());
        assert!(registry.snapshot().is_empty());
        Ok(())
    }

    #[test]
    fn metrics_report_pre_map_rows_after_unfiltered_limit() -> VortexResult<()> {
        let registry: Arc<dyn MetricsRegistry> = Arc::new(DefaultMetricsRegistry::default());
        let tasks = limit_tasks(vec![output_task([1, 2, 3])], 3);
        let stats = ScanStats {
            morsels_after_selection: 2,
            morsels_after_limit: 1,
            ..ScanStats::default()
        };
        let output =
            block_on(instrumented_stream(tasks, stats, Arc::clone(&registry)).collect::<Vec<_>>());
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0]
                .as_ref()
                .map_err(|err| vortex_err!("{err}"))?
                .len(),
            3
        );
        assert_eq!(
            counter(registry.as_ref(), "morsel_scan.output.rows_before_map")?,
            3
        );
        assert_eq!(
            counter(
                registry.as_ref(),
                "morsel_scan.executor_limit.morsels_remaining"
            )?,
            1
        );
        Ok(())
    }

    #[test]
    fn metrics_report_pre_map_rows_after_filtered_limit() -> VortexResult<()> {
        let registry: Arc<dyn MetricsRegistry> = Arc::new(DefaultMetricsRegistry::default());
        let tasks = limit_tasks(vec![output_task([1, 2]), output_task([3, 4])], 3);
        let stats = ScanStats {
            morsels_after_selection: 2,
            morsels_after_limit: 2,
            ..ScanStats::default()
        };
        let output =
            block_on(instrumented_stream(tasks, stats, Arc::clone(&registry)).collect::<Vec<_>>());
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0]
                .as_ref()
                .map_err(|err| vortex_err!("{err}"))?
                .len(),
            3
        );
        assert_eq!(
            counter(registry.as_ref(), "morsel_scan.output.rows_before_map")?,
            3
        );
        assert_eq!(
            counter(
                registry.as_ref(),
                "morsel_scan.executor_limit.morsels_remaining"
            )?,
            2
        );
        Ok(())
    }

    #[test]
    fn same_partition_scans_publish_only_safely_additive_metrics() -> VortexResult<()> {
        let registry: Arc<dyn MetricsRegistry> = Arc::new(DefaultMetricsRegistry::default());
        let metrics = Arc::new(MorselScanMetrics::new(
            Arc::clone(&registry),
            vec![Label::new("partition", "3")],
        ));
        let mut metric_counts = Vec::new();
        for (io_bytes, io_wait) in [(5, Duration::from_nanos(7)), (11, Duration::from_nanos(13))] {
            let stats = ScanStats {
                io_bytes,
                io_wait_time: io_wait,
                io_cells_live_max: 99,
                time_to_first_batch: Some(Duration::from_millis(2)),
                ..ScanStats::default()
            };
            let mut stream =
                instrumented_stream_with_metrics(Vec::new(), stats, Arc::clone(&metrics));
            assert!(block_on(stream.next()).is_none());
            metric_counts.push(registry.snapshot().len());
        }
        assert_eq!(metric_counts, [SCAN_COUNTER_NAMES.len(); 2]);

        let snapshot = registry.snapshot();
        let io_bytes = snapshot
            .iter()
            .filter(|metric| metric.name().as_ref() == "morsel_scan.io.bytes")
            .map(|metric| match metric.value() {
                MetricValue::Counter(counter) => counter.value(),
                _ => 0,
            })
            .sum::<u64>();
        assert_eq!(io_bytes, 16);
        let wait_nanoseconds = snapshot
            .iter()
            .filter(|metric| metric.name().as_ref() == "morsel_scan.io.wait_nanoseconds")
            .map(|metric| match metric.value() {
                MetricValue::Counter(counter) => counter.value(),
                _ => 0,
            })
            .sum::<u64>();
        assert_eq!(wait_nanoseconds, 20);
        assert!(snapshot.iter().all(|metric| {
            !metric.name().starts_with("morsel_scan.")
                || matches!(metric.value(), MetricValue::Counter(_))
        }));
        assert!(snapshot.iter().all(|metric| {
            metric.labels().len() == 1 && metric.labels()[0].key() == "partition"
        }));
        Ok(())
    }

    #[test]
    fn separate_partitions_register_separate_stable_counter_sets() {
        let registry: Arc<dyn MetricsRegistry> = Arc::new(DefaultMetricsRegistry::default());
        for partition in ["1", "2"] {
            let metrics = Arc::new(MorselScanMetrics::new(
                Arc::clone(&registry),
                vec![Label::new("partition", partition)],
            ));
            let mut stream =
                instrumented_stream_with_metrics(Vec::new(), ScanStats::default(), metrics);
            assert!(block_on(stream.next()).is_none());
        }
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), 2 * SCAN_COUNTER_NAMES.len());
        for partition in ["1", "2"] {
            assert_eq!(
                snapshot
                    .iter()
                    .filter(|metric| metric
                        .labels()
                        .iter()
                        .any(|label| { label.key() == "partition" && label.value() == partition }))
                    .count(),
                SCAN_COUNTER_NAMES.len()
            );
        }
    }
}
