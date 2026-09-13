// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use arrow_array::RecordBatchOptions;
use arrow_schema::Field;
use arrow_schema::Schema;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_common::Statistics;
use datafusion_common::arrow::array::AsArray;
use datafusion_common::arrow::array::RecordBatch;
use datafusion_common::exec_datafusion_err;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::TableSchema;
use datafusion_datasource::file_stream::FileOpenFuture;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_execution::cache::cache_manager::CachedFileMetadataEntry;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr::split_conjunction;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::metrics::MetricBuilder;
use datafusion_physical_plan::metrics::MetricCategory;
use datafusion_pruning::FilePruner;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use futures::stream::BoxStream;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::path::Path;
use parking_lot::Mutex;
use tracing::Instrument;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::Chunked;
use vortex::array::arrays::chunked::ChunkedArrayExt;
#[cfg(any(unix, windows))]
use vortex::array::memory::MemorySessionExt;
use vortex::error::VortexError;
use vortex::error::VortexExpect;
use vortex::error::VortexResult;
use vortex::expr::BoundExpression;
use vortex::file::OpenOptionsSessionExt;
use vortex::io::InstrumentedReadAt;
use vortex::io::VortexReadAt;
#[cfg(any(unix, windows))]
use vortex::io::object_store::ObjectStoreReadAt;
#[cfg(any(unix, windows))]
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::LayoutReader;
use vortex::layout::scan::scan_builder::ScanBuilder;
use vortex::layout::segments::SegmentSource;
use vortex::metrics::Counter as VortexCounter;
use vortex::metrics::Gauge as VortexGauge;
use vortex::metrics::Label;
use vortex::metrics::MetricBuilder as VortexMetricBuilder;
use vortex::metrics::MetricsRegistry;
use vortex::scan::selection::Selection;
use vortex::session::VortexSession;
use vortex_arrow::ArrowSessionExt;
use vortex_morsel_scan::MorselScanBuilder;
use vortex_morsel_scan::MorselScanMetrics;
use vortex_morsel_scan::ScanBackend;
use vortex_morsel_scan::ScanExecutorOptions;
use vortex_morsel_scan::scan_backend_from_env;
use vortex_utils::aliases::dash_map::DashMap;
use vortex_utils::aliases::dash_map::Entry;
use vortex_utils::aliases::hash_map::HashMap;

static SCAN_DIAGNOSTICS_ENABLED: OnceLock<bool> = OnceLock::new();

fn scan_diagnostics_enabled() -> bool {
    *SCAN_DIAGNOSTICS_ENABLED.get_or_init(|| {
        std::env::var_os("VORTEX_SCAN_DIAGNOSTICS").is_some_and(|value| value != "0")
    })
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SegmentSourceIdentity {
    object_store: usize,
    location: Path,
    size: u64,
    last_modified_seconds: i64,
    last_modified_nanoseconds: u32,
    e_tag: Option<String>,
    version: Option<String>,
}

impl SegmentSourceIdentity {
    fn new(object_store: usize, metadata: &ObjectMeta) -> Self {
        Self {
            object_store,
            location: metadata.location.clone(),
            size: metadata.size,
            last_modified_seconds: metadata.last_modified.timestamp(),
            last_modified_nanoseconds: metadata.last_modified.timestamp_subsec_nanos(),
            e_tag: metadata.e_tag.clone(),
            version: metadata.version.clone(),
        }
    }
}

#[derive(Default)]
pub(crate) struct PushFrontierSegmentSourcePool {
    entries: Mutex<HashMap<SegmentSourceIdentity, SharedFileSourceEntry>>,
}

struct SharedFileSourceEntry {
    source: Weak<dyn SegmentSource>,
    natural_splits: Arc<Mutex<Option<Arc<NaturalSplits>>>>,
}

impl PushFrontierSegmentSourcePool {
    /// Return the live source for `identity`, constructing one with `source` only on a miss so
    /// partitions that join an existing source never materialize their own segment specs.
    fn get_or_insert(
        &self,
        identity: SegmentSourceIdentity,
        source: impl FnOnce() -> Arc<dyn SegmentSource>,
        max_entries: usize,
    ) -> Arc<dyn SegmentSource> {
        let mut entries = self.entries.lock();
        if let Some(shared) = entries
            .get(&identity)
            .and_then(|entry| entry.source.upgrade())
        {
            return shared;
        }

        let source = source();
        entries.remove(&identity);
        let max_entries = max_entries.max(1);
        if entries.len() >= max_entries {
            entries.retain(|_, entry| entry.source.strong_count() != 0);
        }
        if entries.len() < max_entries {
            entries.insert(
                identity,
                SharedFileSourceEntry {
                    source: Arc::downgrade(&source),
                    natural_splits: Arc::new(Mutex::new(None)),
                },
            );
        }
        source
    }

    fn natural_split_slot(
        &self,
        identity: &SegmentSourceIdentity,
    ) -> Option<Arc<Mutex<Option<Arc<NaturalSplits>>>>> {
        self.entries
            .lock()
            .get(identity)
            .filter(|entry| entry.source.strong_count() != 0)
            .map(|entry| Arc::clone(&entry.natural_splits))
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

#[derive(Clone)]
pub(crate) struct PushFrontierSourceSharing {
    pool: Arc<PushFrontierSegmentSourcePool>,
    /// Keeps pointer identity live for as long as a sharing context can create cache keys. Pool
    /// entries also retain readers that own the same store while their weak source is upgradeable.
    object_store: Arc<dyn ObjectStore>,
    max_entries: usize,
}

impl PushFrontierSourceSharing {
    pub(crate) fn new(
        pool: Arc<PushFrontierSegmentSourcePool>,
        object_store: &Arc<dyn ObjectStore>,
        max_entries: usize,
    ) -> Self {
        Self {
            pool,
            object_store: Arc::clone(object_store),
            max_entries: max_entries.max(1),
        }
    }

    fn identity(&self, metadata: &ObjectMeta) -> SegmentSourceIdentity {
        SegmentSourceIdentity::new(
            Arc::as_ptr(&self.object_store) as *const () as usize,
            metadata,
        )
    }

    fn share(
        &self,
        metadata: &ObjectMeta,
        source: impl FnOnce() -> Arc<dyn SegmentSource>,
    ) -> Arc<dyn SegmentSource> {
        self.pool
            .get_or_insert(self.identity(metadata), source, self.max_entries)
    }

    #[cfg(any(unix, windows))]
    fn create_promoting_reader(
        &self,
        file: &PartitionedFile,
        session: &VortexSession,
        diagnostics: bool,
    ) -> DFResult<Arc<dyn VortexReadAt>> {
        let reader = ObjectStoreReadAt::new_with_allocator(
            Arc::clone(&self.object_store),
            file.path().clone(),
            session.handle(),
            session.allocator(),
        )
        .with_file_payload_promotion(file.object_meta.clone(), diagnostics)
        .map_err(|error| {
            exec_datafusion_err!("Failed to configure persistent local file reader: {error}")
        })?;
        Ok(Arc::new(reader))
    }
}

pub(crate) struct NaturalSplitDiagnostics {
    calls: VortexCounter,
    cache_reuses: VortexCounter,
    lock_contended_acquisitions: VortexCounter,
    lock_wait_nanoseconds: VortexCounter,
    lock_wait_nanoseconds_max: VortexGauge,
    successful_builds: VortexCounter,
    failed_builds: VortexCounter,
    build_nanoseconds: VortexCounter,
    build_nanoseconds_max: VortexGauge,
}

impl NaturalSplitDiagnostics {
    fn new(registry: &dyn MetricsRegistry, labels: &[Label]) -> Self {
        let counter = |name| {
            VortexMetricBuilder::new(registry)
                .add_labels(labels.iter().cloned())
                .counter(name)
        };
        let gauge = |name| {
            VortexMetricBuilder::new(registry)
                .add_labels(labels.iter().cloned())
                .gauge(name)
        };
        Self {
            calls: counter("vortex.scan.natural_splits.calls"),
            cache_reuses: counter("vortex.scan.natural_splits.cache_reuses"),
            lock_contended_acquisitions: counter(
                "vortex.scan.natural_splits.lock_contended_acquisitions",
            ),
            lock_wait_nanoseconds: counter("vortex.scan.natural_splits.lock_wait_nanoseconds"),
            lock_wait_nanoseconds_max: gauge(
                "vortex.scan.natural_splits.lock_wait_nanoseconds_max",
            ),
            successful_builds: counter("vortex.scan.natural_splits.successful_builds"),
            failed_builds: counter("vortex.scan.natural_splits.failed_builds"),
            build_nanoseconds: counter("vortex.scan.natural_splits.build_nanoseconds"),
            build_nanoseconds_max: gauge("vortex.scan.natural_splits.build_nanoseconds_max"),
        }
    }

    fn record(
        &self,
        cache_reused: bool,
        lock_wait: Option<Duration>,
        build_time: Option<Duration>,
        build_succeeded: Option<bool>,
    ) {
        self.calls.add(1);
        if cache_reused {
            self.cache_reuses.add(1);
        }
        if let Some(wait) = lock_wait {
            let nanos = u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX);
            self.lock_contended_acquisitions.add(1);
            self.lock_wait_nanoseconds.add(nanos);
            self.lock_wait_nanoseconds_max.set_max(nanos as f64);
        }
        if let Some(build) = build_time {
            let nanos = u64::try_from(build.as_nanos()).unwrap_or(u64::MAX);
            self.build_nanoseconds.add(nanos);
            self.build_nanoseconds_max.set_max(nanos as f64);
        }
        match build_succeeded {
            Some(true) => self.successful_builds.add(1),
            Some(false) => self.failed_builds.add(1),
            None => {}
        }
    }
}

use crate::VortexAccessPlan;
use crate::convert::exprs::ExpressionConvertor;
use crate::convert::exprs::ProcessedProjection;
use crate::convert::exprs::make_vortex_predicate;
use crate::convert::schema::calculate_physical_schema;
use crate::metrics::PARTITION_LABEL;
use crate::metrics::PATH_LABEL;
use crate::persistent::cache::CachedVortexMetadata;
use crate::persistent::reader::VortexReaderFactory;
use crate::persistent::stream::PrunableStream;

enum FileScanBuilder<A> {
    V1(ScanBuilder<A>),
    Morsel(MorselScanBuilder<A>),
}

#[derive(Debug, Eq, PartialEq)]
struct ScanExecutionConfig {
    builder_concurrency: Option<usize>,
    push_threads: usize,
}

fn scan_execution_config(
    _backend: ScanBackend,
    requested_concurrency: Option<usize>,
) -> ScanExecutionConfig {
    if requested_concurrency.is_none() {
        return ScanExecutionConfig {
            builder_concurrency: None,
            push_threads: 1,
        };
    }

    // DataFusion already partitions the query across the requested number of Tokio workers. Keep
    // each opened file at one unit of scan concurrency, rather than applying the same requested
    // value again as the builders' per-worker multiplier or as extra push executor workers.
    ScanExecutionConfig {
        builder_concurrency: Some(1),
        push_threads: 1,
    }
}

fn env_flag_enabled(cell: &'static OnceLock<bool>, name: &str) -> bool {
    *cell.get_or_init(|| std::env::var_os(name).is_none_or(|value| value != "0"))
}

/// `VORTEX_PF_SOURCE_SHARING=0` disables push-frontier raw source sharing, and everything gated
/// on it, so the backend can be compared with V1 under identical per-partition I/O paths.
fn push_frontier_source_sharing_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    env_flag_enabled(&ENABLED, "VORTEX_PF_SOURCE_SHARING")
}

/// `VORTEX_PF_FILE_PROMOTION=0` keeps source sharing but disables the persistent local
/// file-payload reader, which V1 does not use.
fn push_frontier_file_promotion_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    env_flag_enabled(&ENABLED, "VORTEX_PF_FILE_PROMOTION")
}

fn use_bounded_coalescing_gap(backend: ScanBackend, source_sharing_configured: bool) -> bool {
    backend == ScanBackend::PushFrontier && source_sharing_configured
}

fn use_file_payload_promotion(backend: ScanBackend, source_sharing_configured: bool) -> bool {
    backend == ScanBackend::PushFrontier && source_sharing_configured
}

impl<A: 'static + Send> FileScanBuilder<A> {
    fn with_projection(self, projection: BoundExpression) -> Self {
        match self {
            Self::V1(builder) => Self::V1(builder.with_projection(projection)),
            Self::Morsel(builder) => Self::Morsel(builder.with_projection(projection)),
        }
    }

    fn with_some_filter(self, filter: Option<BoundExpression>) -> Self {
        match self {
            Self::V1(builder) => Self::V1(builder.with_some_filter(filter)),
            Self::Morsel(builder) => Self::Morsel(builder.with_some_filter(filter)),
        }
    }

    fn with_selection(self, selection: Selection) -> Self {
        match self {
            Self::V1(builder) => Self::V1(builder.with_selection(selection)),
            Self::Morsel(builder) => Self::Morsel(builder.with_selection(selection)),
        }
    }

    fn with_limit(self, limit: u64) -> Self {
        match self {
            Self::V1(builder) => Self::V1(builder.with_limit(limit)),
            Self::Morsel(builder) => Self::Morsel(builder.with_limit(limit)),
        }
    }

    fn with_concurrency(self, concurrency: usize) -> Self {
        match self {
            Self::V1(builder) => Self::V1(builder.with_concurrency(concurrency)),
            Self::Morsel(builder) => Self::Morsel(builder.with_concurrency(concurrency)),
        }
    }

    fn with_row_range(self, row_range: Range<u64>) -> Self {
        match self {
            Self::V1(builder) => Self::V1(builder.with_row_range(row_range)),
            Self::Morsel(builder) => Self::Morsel(builder.with_row_range(row_range)),
        }
    }

    fn with_natural_splits(self, boundaries: Arc<[u64]>) -> Self {
        match self {
            Self::V1(builder) => Self::V1(builder.with_natural_splits(boundaries)),
            // Morsel plans already own these boundaries and do not need a reader-side hint.
            Self::Morsel(builder) => Self::Morsel(builder),
        }
    }

    fn with_metrics_registry(
        self,
        metrics: Arc<dyn MetricsRegistry>,
        morsel_metrics: Arc<MorselScanMetrics>,
    ) -> Self {
        match self {
            Self::V1(builder) => Self::V1(builder.with_metrics_registry(metrics)),
            Self::Morsel(builder) => Self::Morsel(builder.with_scan_metrics(morsel_metrics)),
        }
    }

    fn with_ordered(self, ordered: bool) -> Self {
        match self {
            Self::V1(builder) => Self::V1(builder.with_ordered(ordered)),
            Self::Morsel(builder) => Self::Morsel(builder.with_ordered(ordered)),
        }
    }

    fn full_file_splits(&self) -> VortexResult<Vec<u64>> {
        match self {
            Self::V1(builder) => builder.full_file_splits(),
            Self::Morsel(builder) => builder.full_file_splits(),
        }
    }

    fn map<B: 'static + Send>(
        self,
        map_fn: impl Fn(A) -> VortexResult<B> + 'static + Send + Sync,
    ) -> FileScanBuilder<B> {
        match self {
            Self::V1(builder) => FileScanBuilder::V1(builder.map(map_fn)),
            Self::Morsel(builder) => FileScanBuilder::Morsel(builder.map(map_fn)),
        }
    }

    fn into_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        match self {
            Self::V1(builder) => Ok(builder.into_stream()?.boxed()),
            Self::Morsel(builder) => builder.into_stream(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct VortexOpener {
    /// The partition this opener is assigned to. Only used for labeling metrics.
    pub partition: usize,
    pub session: VortexSession,
    pub vortex_reader_factory: Arc<dyn VortexReaderFactory>,
    /// Raw segment sources shared only by push-frontier scans using the built-in object-store
    /// reader. The weak pool owns neither decoded data nor scan/runtime state.
    pub push_frontier_source_sharing: Option<PushFrontierSourceSharing>,
    /// Optional table schema projection. The indices are w.r.t. the `table_schema`, which is
    /// all fields in the final scan result not including the partition columns.
    pub projection: ProjectionExprs,
    /// Filter expression optimized for pushdown into Vortex scan operations.
    /// This may be a subset of file_pruning_predicate containing only expressions
    /// that Vortex can efficiently evaluate.
    pub filter: Option<PhysicalExprRef>,
    /// Filter expression used by DataFusion's FilePruner to eliminate files based on
    /// statistics and partition values without opening them.
    pub file_pruning_predicate: Option<PhysicalExprRef>,
    pub expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    /// This is the table's schema without partition columns. It may contain fields which do
    /// not exist in the file, and are supplied by the `schema_adapter_factory`.
    pub table_schema: TableSchema,
    /// If provided, the scan will not return more than this many rows.
    pub limit: Option<u64>,
    /// A metrics object for tracking performance of the scan.
    pub metrics_registry: Arc<dyn MetricsRegistry>,
    /// Lazily registered push counters shared by every file/range opened for this partition.
    pub morsel_scan_metrics: Arc<OnceLock<Arc<MorselScanMetrics>>>,
    /// Lazily registered diagnostic metrics shared by every open for this partition.
    pub natural_split_diagnostics: Arc<OnceLock<Arc<NaturalSplitDiagnostics>>>,
    /// DataFusion-native metrics exposed through `DataSourceExec`.
    pub df_metrics: ExecutionPlanMetricsSet,
    /// A shared cache of file readers.
    ///
    /// To save on the overhead of reparsing FlatBuffers and rebuilding the layout tree, we cache
    /// a file reader the first time we read a file.
    pub layout_readers: Arc<DashMap<Path, Weak<dyn LayoutReader>>>,
    /// Shared full-file natural splits keyed by file path.
    pub natural_splits: Arc<DashMap<Path, Arc<NaturalSplits>>>,
    /// Whether the query has output ordering specified
    pub has_output_ordering: bool,

    pub expression_convertor: Arc<dyn ExpressionConvertor>,
    pub file_metadata_cache: Option<Arc<FileMetadataCache>>,
    /// Whether to enable expression pushdown into the underlying Vortex scan.
    pub projection_pushdown: bool,
    pub scan_concurrency: Option<usize>,
}

impl VortexOpener {
    fn morsel_scan_metrics(&self) -> Arc<MorselScanMetrics> {
        Arc::clone(self.morsel_scan_metrics.get_or_init(|| {
            Arc::new(MorselScanMetrics::new(
                Arc::clone(&self.metrics_registry),
                vec![Label::new(PARTITION_LABEL, self.partition.to_string())],
            ))
        }))
    }

    fn natural_split_diagnostics(&self) -> Option<Arc<NaturalSplitDiagnostics>> {
        scan_diagnostics_enabled().then(|| {
            let labels = [Label::new(PARTITION_LABEL, self.partition.to_string())];
            Arc::clone(self.natural_split_diagnostics.get_or_init(|| {
                Arc::new(NaturalSplitDiagnostics::new(
                    self.metrics_registry.as_ref(),
                    &labels,
                ))
            }))
        })
    }
}

impl VortexOpener {
    fn open_with_backend(
        &self,
        file: PartitionedFile,
        backend: ScanBackend,
    ) -> DFResult<FileOpenFuture> {
        #[cfg(any(unix, windows))]
        let diagnostics_enabled = scan_diagnostics_enabled();
        let source_sharing_configured =
            self.push_frontier_source_sharing.is_some() && push_frontier_source_sharing_enabled();
        let bounded_coalescing_gap = use_bounded_coalescing_gap(backend, source_sharing_configured);
        let push_frontier_source_sharing =
            use_file_payload_promotion(backend, source_sharing_configured)
                .then(|| self.push_frontier_source_sharing.clone())
                .flatten();
        #[cfg(any(unix, windows))]
        let file_payload_promotion =
            push_frontier_source_sharing.is_some() && push_frontier_file_promotion_enabled();

        // Calculate the output schema before replacing partition columns with literals so it
        // retains the table and partition-field metadata declared by the plan.
        let output_schema = Arc::new(
            self.projection
                .project_schema(self.table_schema.table_schema())?,
        );
        let session = self.session.clone();
        let metrics_registry = Arc::clone(&self.metrics_registry);
        let morsel_scan_metrics = self.morsel_scan_metrics();
        let natural_split_diagnostics = self.natural_split_diagnostics();
        let labels = vec![
            Label::new(PATH_LABEL, file.path().to_string()),
            Label::new(PARTITION_LABEL, self.partition.to_string()),
        ];
        let source_labels = if push_frontier_source_sharing.is_some() {
            vec![Label::new(PATH_LABEL, file.path().to_string())]
        } else {
            labels
        };
        let mut projection = self.projection.clone();
        let mut filter = self.filter.clone();

        #[cfg(any(unix, windows))]
        let reader = match &push_frontier_source_sharing {
            Some(sharing) if file_payload_promotion => {
                sharing.create_promoting_reader(&file, &session, diagnostics_enabled)?
            }
            _ => self.vortex_reader_factory.create_reader(&file, &session)?,
        };
        #[cfg(not(any(unix, windows)))]
        let reader = self.vortex_reader_factory.create_reader(&file, &session)?;

        let reader = InstrumentedReadAt::new_with_labels(
            reader,
            metrics_registry.as_ref(),
            source_labels.clone(),
        );
        let shared_source_max_stale_entries = push_frontier_source_sharing
            .as_ref()
            .map(|_| reader.concurrency().clamp(1, 256).saturating_mul(4).max(16));

        let mut file_pruning_predicate = self.file_pruning_predicate.clone();
        let expr_adapter_factory = Arc::clone(&self.expr_adapter_factory);
        let file_metadata_cache = self.file_metadata_cache.clone();

        let unified_file_schema = Arc::clone(self.table_schema.file_schema());
        let limit = self.limit;
        let layout_readers = Arc::clone(&self.layout_readers);
        let natural_splits = Arc::clone(&self.natural_splits);
        let has_output_ordering = self.has_output_ordering;
        let scan_concurrency = self.scan_concurrency;

        let expr_convertor = Arc::clone(&self.expression_convertor);
        let projection_pushdown = self.projection_pushdown;

        let predicate_creation_errors = MetricBuilder::new(&self.df_metrics)
            .with_category(MetricCategory::Rows)
            .global_counter("num_predicate_creation_errors");

        // Replace column access for partition columns with literals
        #[expect(clippy::disallowed_types)]
        let literal_value_cols = self
            .table_schema
            .table_partition_cols()
            .iter()
            .map(|f| f.name())
            .cloned()
            .zip(file.partition_values.clone())
            .collect::<std::collections::HashMap<String, ScalarValue>>();

        let predicate_uses_partition_columns =
            file_pruning_predicate.as_ref().is_some_and(|predicate| {
                collect_columns(predicate)
                    .iter()
                    .any(|column| literal_value_cols.contains_key(column.name()))
            });

        if !literal_value_cols.is_empty() {
            projection = projection.try_map_exprs(|expr| {
                replace_columns_with_literals(Arc::clone(&expr), &literal_value_cols)
            })?;
            filter = filter
                .map(|p| replace_columns_with_literals(p, &literal_value_cols))
                .transpose()?;
            file_pruning_predicate = file_pruning_predicate
                .map(|p| replace_columns_with_literals(p, &literal_value_cols))
                .transpose()?;
        }

        Ok(async move {
            // FilePruner requires a statistics object even when the rewritten predicate
            // only contains partition literals. Supply unknown file-column statistics in
            // that case so static and dynamic partition predicates can still prune.
            let synthetic_statistics = (!file.has_statistics() && predicate_uses_partition_columns)
                .then(|| {
                    file.clone()
                        .with_statistics(Arc::new(Statistics::new_unknown(&unified_file_schema)))
                });
            let pruning_file = synthetic_statistics.as_ref().unwrap_or(&file);

            let mut file_pruner = file_pruning_predicate
                .filter(|_| file.has_statistics() || predicate_uses_partition_columns)
                .and_then(|predicate| {
                    FilePruner::try_new(
                        Arc::clone(&predicate),
                        &unified_file_schema,
                        pruning_file,
                        predicate_creation_errors,
                    )
                });

            // Check if this file should be pruned based on statistics/partition values.
            // Returns empty stream if file can be skipped entirely.
            if let Some(file_pruner) = file_pruner.as_mut()
                && file_pruner.should_prune()?
            {
                return Ok(stream::empty().boxed());
            }

            let mut open_opts = session
                .open_options()
                .with_file_size(file.object_meta.size)
                .with_metrics_registry(Arc::clone(&metrics_registry))
                .with_labels(source_labels);
            if let Some(max_stale_entries) = shared_source_max_stale_entries {
                open_opts = open_opts.with_shared_source_max_stale_entries(max_stale_entries);
            }
            if bounded_coalescing_gap {
                open_opts = open_opts.with_bounded_coalescing_gap();
            }

            let cached_footer = file_metadata_cache
                .as_ref()
                .and_then(|cache| cache.get(file.path()))
                .filter(|entry| {
                    if push_frontier_source_sharing.is_some() {
                        entry.meta == file.object_meta
                    } else {
                        entry.is_valid_for(&file.object_meta)
                    }
                })
                .and_then(|entry| {
                    entry
                        .file_metadata
                        .as_any()
                        .downcast_ref::<CachedVortexMetadata>()
                        .map(|vortex_metadata| vortex_metadata.footer().clone())
                });
            let footer_cache_hit = cached_footer.is_some();

            if let Some(footer) = cached_footer {
                open_opts = open_opts.with_footer(footer);
            }

            let vxf = open_opts
                .open_read(reader)
                .await
                .map_err(|e| exec_datafusion_err!("Failed to open Vortex file {e}"))?;

            let vxf = if let Some(sharing) = &push_frontier_source_sharing {
                let segment_source = sharing.share(&file.object_meta, || vxf.segment_source());
                vxf.with_segment_source(segment_source)
            } else {
                vxf
            };

            // On a miss, cache the parsed footer so other partitions and later executions
            // skip the footer fetch and parse. `infer_schema`/`infer_stats` also populate
            // this cache, but only when planning goes through `VortexFormat`.
            if !footer_cache_hit && let Some(cache) = &file_metadata_cache {
                cache.put(
                    file.path(),
                    CachedFileMetadataEntry::new(
                        file.object_meta.clone(),
                        Arc::new(CachedVortexMetadata::new(&vxf)),
                    ),
                );
            }

            // Check if there are rows in this file. If not, we can save
            // ourselves some work and return an empty stream.
            if vxf.row_count() == 0 {
                return Ok(stream::empty().boxed());
            }

            // This is the expected arrow types of the actual columns in the file, which might have different types
            // from the unified logical schema or miss
            let this_file_schema = Arc::new(calculate_physical_schema(
                vxf.dtype(),
                &unified_file_schema,
                &session.arrow(),
            )?);

            let expr_adapter = expr_adapter_factory.create(
                Arc::clone(&unified_file_schema),
                Arc::clone(&this_file_schema),
            )?;

            let simplifier = PhysicalExprSimplifier::new(&this_file_schema);

            // The adapter rewrites the expressions to the local file schema, allowing
            // for schema evolution and divergence between the table's schema and individual files.
            let filter = filter
                .map(|filter| {
                    // Expression might now reference columns that don't exist in the file, so we can give it
                    // another simplification pass.
                    simplifier.simplify(expr_adapter.rewrite(filter)?)
                })
                .transpose()?;
            let projection =
                projection.try_map_exprs(|p| simplifier.simplify(expr_adapter.rewrite(p)?))?;

            let ProcessedProjection {
                scan_projection,
                leftover_projection,
            } = if projection_pushdown {
                expr_convertor.split_projection(
                    projection.clone(),
                    &this_file_schema,
                    output_schema.as_ref(),
                )?
            } else {
                // When projection pushdown is disabled, read only the required columns
                // and apply the full projection after the scan.
                expr_convertor.no_pushdown_projection(projection.clone(), &this_file_schema)?
            };

            // The schema of the stream returned from the vortex scan.
            // We use a reference schema for types that don't roundtrip (Dictionary, Utf8, etc.).
            let scan_projection = scan_projection
                .optimize_recursive(vxf.dtype())
                .and_then(|projection| projection.bind(vxf.dtype()))
                .map_err(|_e| {
                    exec_datafusion_err!("Couldn't get the dtype for the underlying Vortex scan")
                })?;
            let scan_dtype = scan_projection.dtype().clone();

            // When projection pushdown is enabled, the scan outputs the projected columns.
            // When disabled, the scan outputs raw columns and the projection is applied after.
            let scan_reference_schema = if projection_pushdown {
                (*output_schema).clone()
            } else {
                // Build schema from the raw columns being read
                let column_indices = projection.column_indices();
                let fields: Vec<_> = column_indices
                    .into_iter()
                    .map(|idx| this_file_schema.field(idx).clone())
                    .collect();
                Schema::new_with_metadata(fields, this_file_schema.metadata().clone())
            };
            let stream_schema =
                calculate_physical_schema(&scan_dtype, &scan_reference_schema, &session.arrow())?;

            let leftover_projection = leftover_projection
                .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;
            let projector = leftover_projection.make_projector(&stream_schema)?;

            let execution_config = scan_execution_config(backend, scan_concurrency);
            let mut scan_builder = match backend {
                ScanBackend::V1 => {
                    // Only V1 constructs and caches a LayoutReader tree.
                    let layout_reader = match layout_readers
                        .entry(file.object_meta.location.clone())
                    {
                        Entry::Occupied(mut occupied_entry) => {
                            if let Some(reader) = occupied_entry.get().upgrade() {
                                tracing::trace!(
                                    "reusing layout reader for {}",
                                    occupied_entry.key()
                                );
                                reader
                            } else {
                                tracing::trace!(
                                    "creating layout reader for {}",
                                    occupied_entry.key()
                                );
                                let reader = vxf.layout_reader().map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "Failed to create layout reader: {e}"
                                    ))
                                })?;
                                occupied_entry.insert(Arc::downgrade(&reader));
                                reader
                            }
                        }
                        Entry::Vacant(vacant_entry) => {
                            tracing::trace!("creating layout reader for {}", vacant_entry.key());
                            let reader = vxf.layout_reader().map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to create layout reader: {e}"
                                ))
                            })?;
                            vacant_entry.insert(Arc::downgrade(&reader));
                            reader
                        }
                    };
                    FileScanBuilder::V1(ScanBuilder::new(session.clone(), layout_reader))
                }
                ScanBackend::Push | ScanBackend::PushFrontier => {
                    let options =
                        ScanExecutorOptions::default().with_threads(execution_config.push_threads);
                    FileScanBuilder::Morsel(
                        MorselScanBuilder::new(
                            session.clone(),
                            backend,
                            Arc::clone(vxf.footer().layout()),
                            vxf.segment_source(),
                            &options,
                        )
                        .map_err(|err| {
                            exec_datafusion_err!("Failed to create morsel scan: {err}")
                        })?,
                    )
                }
            };

            if let Some(vortex_plan) = file.extensions.get::<VortexAccessPlan>()
                && let Some(selection) = vortex_plan.selection()
            {
                scan_builder = scan_builder.with_selection(selection.clone());
            }

            let filter = filter
                .and_then(|f| {
                    // Verify that all filters we've accepted from DataFusion get pushed down.
                    // This will only fail if the user has not configured a suitable
                    // PhysicalExprAdapterFactory on the file source to handle rewriting the
                    // expression to handle missing/reordered columns in the Vortex file.
                    let (pushed, unpushed): (Vec<PhysicalExprRef>, Vec<PhysicalExprRef>) =
                        split_conjunction(&f)
                            .into_iter()
                            .cloned()
                            .partition(|expr| {
                                expr_convertor.can_be_pushed_down(expr, &this_file_schema)
                            });

                    if !unpushed.is_empty() {
                        return Some(Err(exec_datafusion_err!(
                            r#"VortexSource accepted but failed to push {} filters.
                            This should never happen if you have a properly configured
                            PhysicalExprAdapterFactory configured on the source.

                            Failed filters:

                            {unpushed:#?}
                            "#,
                            unpushed.len()
                        )));
                    }

                    make_vortex_predicate(expr_convertor.as_ref(), &pushed).transpose()
                })
                .transpose()?;
            let filter = filter
                .map(|filter| filter.optimize_recursive(vxf.dtype())?.bind(vxf.dtype()))
                .transpose()
                .map_err(|e| exec_datafusion_err!("Couldn't bind Vortex scan filter: {e}"))?;

            if let Some(limit) = limit
                && filter.is_none()
            {
                scan_builder = scan_builder.with_limit(limit);
            }

            if let Some(concurrency) = execution_config.builder_concurrency {
                scan_builder = scan_builder.with_concurrency(concurrency);
            }

            // Set before the byte-range translation below, which computes natural splits for
            // the fields the scan's projection and filter reference.
            scan_builder = scan_builder
                .with_projection(scan_projection)
                .with_some_filter(filter);

            if let Some(file_range) = file.range {
                let byte_range = Range {
                    start: u64::try_from(file_range.start)
                        .map_err(|_| exec_datafusion_err!("Vortex file range start is negative"))?,
                    end: u64::try_from(file_range.end)
                        .map_err(|_| exec_datafusion_err!("Vortex file range end is negative"))?,
                };
                if byte_range.start != 0 || byte_range.end != file.object_meta.size {
                    // Full-file scans already cover every natural split. Only translate the
                    // byte range back into row boundaries when DataFusion has trimmed the file.
                    let natural_splits = if let Some(sharing) = &push_frontier_source_sharing {
                        push_frontier_natural_splits_for_file(
                            sharing,
                            &file.object_meta,
                            &scan_builder,
                            natural_split_diagnostics.as_deref(),
                        )?
                    } else {
                        natural_splits_for_file(
                            natural_splits.as_ref(),
                            &file.object_meta.location,
                            &scan_builder,
                            file.object_meta.size,
                            natural_split_diagnostics.as_deref(),
                        )?
                    };

                    let Some(row_range) =
                        split_aligned_row_range(byte_range, natural_splits.as_ref())
                    else {
                        return Ok(stream::empty().boxed());
                    };

                    scan_builder = scan_builder
                        .with_row_range(row_range)
                        // Hand the shared full-file boundaries back to the scan so prepare()
                        // skips its own layout walk.
                        .with_natural_splits(Arc::clone(&natural_splits.row_boundaries));
                }
            }

            let stream_target_field = Field::new_struct("", stream_schema.fields().clone(), false);
            let stream = scan_builder
                .with_metrics_registry(metrics_registry, morsel_scan_metrics)
                .with_ordered(has_output_ordering)
                .map(move |chunk| {
                    let mut ctx = session.create_execution_ctx();
                    let arrow_session = ctx.session().clone();
                    // A scan unit spanning several segments arrives as a chunked struct.
                    // Converting each chunk on its own avoids re-concatenating every column
                    // into one array before the Arrow conversion.
                    let chunks = match chunk.as_opt::<Chunked>() {
                        Some(chunked) => chunked.chunks(),
                        None => vec![chunk],
                    };
                    chunks
                        .into_iter()
                        .filter(|chunk| !chunk.is_empty())
                        .map(|chunk| {
                            let arrow = arrow_session.arrow().execute_arrow(
                                chunk,
                                Some(&stream_target_field),
                                &mut ctx,
                            )?;
                            Ok(RecordBatch::from(arrow.as_struct().clone()))
                        })
                        .collect::<VortexResult<Vec<_>>>()
                })
                .into_stream()
                .map_err(|e| exec_datafusion_err!("Failed to create Vortex stream: {e}"))?
                .map_ok(|batches| stream::iter(batches.into_iter().map(Ok::<_, VortexError>)))
                .try_flatten()
                .map_err(move |e: VortexError| {
                    DataFusionError::External(Box::new(e.with_context(format!(
                        "Failed to read Vortex file: {}",
                        file.object_meta.location
                    ))))
                })
                .map(move |batch| {
                    let batch = if projector.projection().as_ref().is_empty() {
                        batch
                    } else {
                        batch.and_then(|b| projector.project_batch(&b))
                    }?;

                    let (_, columns, row_count) = batch.into_parts();
                    RecordBatch::try_new_with_options(
                        Arc::clone(&output_schema),
                        columns,
                        &RecordBatchOptions::new().with_row_count(Some(row_count)),
                    )
                    .map_err(Into::into)
                })
                .boxed();

            if let Some(file_pruner) = file_pruner {
                Ok(PrunableStream::new(file_pruner, stream).boxed())
            } else {
                Ok(stream)
            }
        }
        .in_current_span()
        .boxed())
    }
}

impl FileOpener for VortexOpener {
    fn open(&self, file: PartitionedFile) -> DFResult<FileOpenFuture> {
        let backend = scan_backend_from_env()
            .map_err(|err| exec_datafusion_err!("Invalid scan backend: {err}"))?;
        self.open_with_backend(file, backend)
    }
}

/// A file's natural split boundaries plus the precomputed byte each split is assigned to,
/// enabling [`split_aligned_row_range`] to translate a DataFusion byte range into row
/// boundaries with a binary search instead of re-projecting every split per partition.
///
/// The boundaries are computed for the fields referenced by the scan's projection and filter.
/// All partitions translate through the first opener's cached entry (the cache lives on the
/// source, so projection and filter are fixed for its lifetime), which keeps the byte ranges
/// tiling the file's rows exactly once.
#[derive(Debug)]
pub(crate) struct NaturalSplits {
    /// Sorted row boundaries of the natural splits; split `i` covers
    /// `row_boundaries[i]..row_boundaries[i + 1]`. Shared so partitions can hand the
    /// boundaries back to the V1 scan builder, skipping its per-partition layout walk.
    row_boundaries: Arc<[u64]>,
    /// For each split, the byte a DataFusion byte range must contain to own it (see
    /// [`split_assignment_byte`]); one entry per split, sorted because split midpoints
    /// increase monotonically under the row-to-byte projection.
    assignment_bytes: Box<[u64]>,
}

impl NaturalSplits {
    fn new(row_boundaries: Arc<[u64]>, total_size: u64) -> Self {
        let row_count = row_boundaries.last().copied().unwrap_or_default();
        let assignment_bytes = if row_count == 0 {
            Box::default()
        } else {
            row_boundaries
                .windows(2)
                .enumerate()
                .map(|(idx, boundaries)| {
                    split_assignment_byte(
                        idx,
                        &(boundaries[0]..boundaries[1]),
                        row_count,
                        total_size,
                    )
                })
                .collect()
        };

        debug_assert!(assignment_bytes.is_sorted());
        debug_assert_eq!(
            assignment_bytes.len() + usize::from(!row_boundaries.is_empty()),
            row_boundaries.len()
        );

        Self {
            row_boundaries,
            assignment_bytes,
        }
    }
}

/// Return natural splits associated with the exact immutable object identity used by a pooled
/// push-frontier source. The slot lives only in a concurrency-bounded pool entry.
fn push_frontier_natural_splits_for_file<A: 'static + Send>(
    sharing: &PushFrontierSourceSharing,
    metadata: &ObjectMeta,
    scan_builder: &FileScanBuilder<A>,
    diagnostics: Option<&NaturalSplitDiagnostics>,
) -> DFResult<Arc<NaturalSplits>> {
    let identity = sharing.identity(metadata);
    let Some(slot) = sharing.pool.natural_split_slot(&identity) else {
        let started = Instant::now();
        let result = compute_natural_splits(scan_builder, metadata.size);
        if let Some(diagnostics) = diagnostics {
            diagnostics.record(false, None, Some(started.elapsed()), Some(result.is_ok()));
        }
        return result;
    };

    if diagnostics.is_none() {
        let mut cached = slot.lock();
        if let Some(splits) = cached.as_ref() {
            return Ok(Arc::clone(splits));
        }
        let splits = compute_natural_splits(scan_builder, metadata.size)?;
        *cached = Some(Arc::clone(&splits));
        return Ok(splits);
    }

    let diagnostics = diagnostics.vortex_expect("diagnostics were checked above");
    let (mut cached, lock_wait) = match slot.try_lock() {
        Some(cached) => (cached, None),
        None => {
            let started = Instant::now();
            let cached = slot.lock();
            (cached, Some(started.elapsed()))
        }
    };
    if let Some(splits) = cached.as_ref() {
        let splits = Arc::clone(splits);
        drop(cached);
        diagnostics.record(true, lock_wait, None, None);
        return Ok(splits);
    }

    let started = Instant::now();
    let result = compute_natural_splits(scan_builder, metadata.size);
    let build_time = started.elapsed();
    if let Ok(splits) = &result {
        *cached = Some(Arc::clone(splits));
    }
    drop(cached);
    diagnostics.record(false, lock_wait, Some(build_time), Some(result.is_ok()));
    result
}

/// Return the cached [`NaturalSplits`] for `path`, computing and caching them on first use.
fn natural_splits_for_file<A: 'static + Send>(
    natural_splits: &DashMap<Path, Arc<NaturalSplits>>,
    path: &Path,
    scan_builder: &FileScanBuilder<A>,
    total_size: u64,
    diagnostics: Option<&NaturalSplitDiagnostics>,
) -> DFResult<Arc<NaturalSplits>> {
    if diagnostics.is_none() {
        if let Some(splits) = natural_splits.get(path) {
            return Ok(Arc::clone(splits.value()));
        }
        return match natural_splits.entry(path.clone()) {
            Entry::Occupied(entry) => Ok(Arc::clone(entry.get())),
            Entry::Vacant(entry) => {
                let splits = compute_natural_splits(scan_builder, total_size)?;
                entry.insert(Arc::clone(&splits));
                Ok(splits)
            }
        };
    }

    let diagnostics = diagnostics.vortex_expect("diagnostics were checked above");
    let initial = natural_splits.try_get(path);
    let initially_locked = initial.is_locked();
    if let Some(splits_ref) = initial.try_unwrap() {
        let splits = Arc::clone(splits_ref.value());
        drop(splits_ref);
        diagnostics.record(true, None, None, None);
        return Ok(splits);
    }

    let (entry, wait_started) = if initially_locked {
        let started = Instant::now();
        (natural_splits.entry(path.clone()), Some(started))
    } else if let Some(entry) = natural_splits.try_entry(path.clone()) {
        (entry, None)
    } else {
        let started = Instant::now();
        (natural_splits.entry(path.clone()), Some(started))
    };
    let lock_wait = wait_started.map(|started| started.elapsed());
    let (result, cache_reused, build_time, build_succeeded) = match entry {
        Entry::Occupied(entry) => (Ok(Arc::clone(entry.get())), true, None, None),
        Entry::Vacant(entry) => {
            let started = Instant::now();
            match compute_natural_splits(scan_builder, total_size) {
                Ok(splits) => {
                    entry.insert(Arc::clone(&splits));
                    (Ok(splits), false, Some(started.elapsed()), Some(true))
                }
                Err(error) => (Err(error), false, Some(started.elapsed()), Some(false)),
            }
        }
    };
    // Do not publish metrics while holding the DashMap shard guard.
    diagnostics.record(cache_reused, lock_wait, build_time, build_succeeded);
    result
}

/// Walk the layout tree to compute the file's full natural split boundaries for the fields
/// referenced by the scan's projection and filter.
fn compute_natural_splits<A: 'static + Send>(
    scan_builder: &FileScanBuilder<A>,
    total_size: u64,
) -> DFResult<Arc<NaturalSplits>> {
    let row_boundaries = scan_builder
        .full_file_splits()
        .map_err(|e| exec_datafusion_err!("Failed to compute Vortex natural splits: {e}"))?;

    Ok(Arc::new(NaturalSplits::new(
        row_boundaries.into(),
        total_size,
    )))
}

/// Translate a DataFusion byte range to the contiguous natural split ranges it owns.
/// Most splits are assigned by midpoint, but the leading split stays with the range that owns
/// byte 0 so a tiny first byte range still claims the first rows.
fn split_aligned_row_range(
    byte_range: Range<u64>,
    natural_splits: &NaturalSplits,
) -> Option<Range<u64>> {
    if byte_range.start >= byte_range.end {
        return None;
    }

    let first_split = natural_splits
        .assignment_bytes
        .partition_point(|&assignment_byte| assignment_byte < byte_range.start);
    let after_last_split = natural_splits
        .assignment_bytes
        .partition_point(|&assignment_byte| assignment_byte < byte_range.end);
    if first_split == after_last_split {
        return None;
    }

    Some(
        natural_splits.row_boundaries[first_split]..natural_splits.row_boundaries[after_last_split],
    )
}

fn split_assignment_byte(
    idx: usize,
    split_range: &Range<u64>,
    row_count: u64,
    total_size: u64,
) -> u64 {
    if idx == 0 && split_range.start == 0 {
        // Byte 0 is the only stable representative for the leading split. A midpoint can fall
        // into the next DataFusion byte range and leave the first range with no rows to read.
        0
    } else {
        split_midpoint_to_byte(split_range, row_count, total_size)
    }
}

fn split_midpoint_to_byte(split_range: &Range<u64>, row_count: u64, total_size: u64) -> u64 {
    let midpoint_row = split_range.start + (split_range.end - split_range.start) / 2;
    let midpoint_byte = (u128::from(midpoint_row) * u128::from(total_size)) / u128::from(row_count);

    u64::try_from(midpoint_byte).vortex_expect("midpoint byte projection should fit into u64")
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::Arc;
    use std::sync::LazyLock;

    use arrow_array::record_batch;
    use arrow_schema::Field;
    use arrow_schema::Fields;
    use arrow_schema::SchemaRef;
    use datafusion::arrow::array::DictionaryArray;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::array::StructArray;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::datatypes::UInt32Type;
    use datafusion::arrow::util::display::FormatOptions;
    use datafusion::arrow::util::pretty::pretty_format_batches_with_options;
    use datafusion::logical_expr::col;
    use datafusion::logical_expr::lit;
    use datafusion::physical_expr::planner::logical2physical;
    use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
    use datafusion::scalar::ScalarValue;
    use datafusion_common::stats::Precision;
    use datafusion_execution::cache::default_cache::DefaultCache;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions as df_expr;
    use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
    use datafusion_physical_expr::projection::ProjectionExpr;
    use insta::assert_snapshot;
    use itertools::Itertools;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use rstest::rstest;
    use vortex::VortexSessionDefault;
    use vortex::array::buffer::BufferHandle;
    use vortex::buffer::Buffer;
    use vortex::buffer::ByteBuffer;
    use vortex::file::WriteOptionsSessionExt;
    use vortex::io::VortexWrite;
    use vortex::io::object_store::ObjectStoreWrite;
    use vortex::layout::LayoutStrategy;
    use vortex::layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex::layout::layouts::table::TableStrategy;
    use vortex::layout::segments::SegmentFuture;
    use vortex::layout::segments::SegmentId;
    use vortex::metrics::DefaultMetricsRegistry;
    use vortex::metrics::MetricValue;
    use vortex::scan::selection::Selection;
    use vortex::scan::strict_sorted_buffer::StrictSortedBuffer;
    use vortex::session::VortexSession;

    use super::*;
    use crate::VortexAccessPlan;
    use crate::convert::exprs::DefaultExpressionConvertor;
    use crate::persistent::reader::DefaultVortexReaderFactory;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::default);

    struct StaticSegmentSource(u8);

    impl SegmentSource for StaticSegmentSource {
        fn request(&self, _id: SegmentId) -> SegmentFuture {
            futures::future::ready(Ok(BufferHandle::new_host(ByteBuffer::from(vec![self.0]))))
                .boxed()
        }
    }

    fn test_object_meta() -> anyhow::Result<ObjectMeta> {
        Ok(ObjectMeta {
            location: Path::from("versioned.vortex"),
            last_modified: "2026-09-12T12:34:56.123456789Z".parse()?,
            size: 1234,
            e_tag: Some("etag-a".to_owned()),
            version: Some("version-a".to_owned()),
        })
    }

    #[test]
    fn segment_source_identity_includes_store_and_all_object_metadata() -> anyhow::Result<()> {
        let first_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let second_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let first_store_id = Arc::as_ptr(&first_store) as *const () as usize;
        let second_store_id = Arc::as_ptr(&second_store) as *const () as usize;
        let metadata = test_object_meta()?;
        let identity = SegmentSourceIdentity::new(first_store_id, &metadata);

        assert_ne!(
            identity,
            SegmentSourceIdentity::new(second_store_id, &metadata)
        );
        let mut changed = metadata.clone();
        changed.location = Path::from("other.vortex");
        assert_ne!(
            identity,
            SegmentSourceIdentity::new(first_store_id, &changed)
        );
        let mut changed = metadata.clone();
        changed.size += 1;
        assert_ne!(
            identity,
            SegmentSourceIdentity::new(first_store_id, &changed)
        );
        let mut changed = metadata.clone();
        changed.last_modified = "2026-09-12T12:34:56.123456790Z".parse()?;
        assert_ne!(
            identity,
            SegmentSourceIdentity::new(first_store_id, &changed)
        );
        let mut changed = metadata.clone();
        changed.e_tag = Some("etag-b".to_owned());
        assert_ne!(
            identity,
            SegmentSourceIdentity::new(first_store_id, &changed)
        );
        let mut changed = metadata;
        changed.version = Some("version-b".to_owned());
        assert_ne!(
            identity,
            SegmentSourceIdentity::new(first_store_id, &changed)
        );
        Ok(())
    }

    #[tokio::test]
    async fn source_pool_shares_exact_source_for_one_identity() -> anyhow::Result<()> {
        let pool = PushFrontierSegmentSourcePool::default();
        let identity = SegmentSourceIdentity::new(1, &test_object_meta()?);
        let first = Arc::new(StaticSegmentSource(7)) as Arc<dyn SegmentSource>;
        let selected = pool.get_or_insert(identity.clone(), || Arc::clone(&first), 2);
        let second = Arc::new(StaticSegmentSource(9)) as Arc<dyn SegmentSource>;
        let selected_again = pool.get_or_insert(identity, || second, 2);

        assert!(Arc::ptr_eq(&selected, &selected_again));
        let (left, right) = futures::join!(
            selected.request(SegmentId::from(0)),
            selected_again.request(SegmentId::from(0))
        );
        assert_eq!(left?.unwrap_host().as_ref(), &[7]);
        assert_eq!(right?.unwrap_host().as_ref(), &[7]);
        Ok(())
    }

    #[test]
    fn source_pool_concurrently_selects_one_same_identity_source() -> anyhow::Result<()> {
        let pool = Arc::new(PushFrontierSegmentSourcePool::default());
        let identity = SegmentSourceIdentity::new(1, &test_object_meta()?);
        let barrier = Arc::new(std::sync::Barrier::new(32));

        let selected = std::thread::scope(|scope| {
            let handles = (0..32)
                .map(|value| {
                    let pool = Arc::clone(&pool);
                    let identity = identity.clone();
                    let barrier = Arc::clone(&barrier);
                    scope.spawn(move || {
                        let candidate =
                            Arc::new(StaticSegmentSource(value)) as Arc<dyn SegmentSource>;
                        barrier.wait();
                        pool.get_or_insert(identity, || candidate, 4)
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
                })
                .collect::<Vec<_>>()
        });

        assert_eq!(pool.len(), 1);
        assert!(
            selected[1..]
                .iter()
                .all(|source| Arc::ptr_eq(&selected[0], source))
        );
        Ok(())
    }

    #[test]
    fn source_pool_cleans_dead_keys_and_respects_concurrency_bound() -> anyhow::Result<()> {
        let pool = PushFrontierSegmentSourcePool::default();
        let metadata = test_object_meta()?;
        let first_identity = SegmentSourceIdentity::new(1, &metadata);
        let mut second_metadata = metadata.clone();
        second_metadata.location = Path::from("second.vortex");
        let second_identity = SegmentSourceIdentity::new(1, &second_metadata);
        let mut third_metadata = metadata;
        third_metadata.location = Path::from("third.vortex");
        let third_identity = SegmentSourceIdentity::new(1, &third_metadata);

        let first = pool.get_or_insert(first_identity, || Arc::new(StaticSegmentSource(1)), 2);
        let _second = pool.get_or_insert(second_identity, || Arc::new(StaticSegmentSource(2)), 2);
        let uncached = pool.get_or_insert(
            third_identity.clone(),
            || Arc::new(StaticSegmentSource(3)),
            2,
        );
        assert_eq!(pool.len(), 2);

        drop(first);
        let cached = pool.get_or_insert(
            third_identity.clone(),
            || Arc::new(StaticSegmentSource(4)),
            2,
        );
        assert_eq!(pool.len(), 2);
        assert!(!Arc::ptr_eq(&uncached, &cached));
        let selected_again =
            pool.get_or_insert(third_identity, || Arc::new(StaticSegmentSource(5)), 2);
        assert!(Arc::ptr_eq(&cached, &selected_again));
        Ok(())
    }

    #[rstest]
    #[case(ScanBackend::V1, true, false)]
    #[case(ScanBackend::Push, true, false)]
    #[case(ScanBackend::PushFrontier, false, false)]
    #[case(ScanBackend::PushFrontier, true, true)]
    fn bounded_coalescing_gap_is_isolated_to_shared_push_frontier(
        #[case] backend: ScanBackend,
        #[case] source_sharing_configured: bool,
        #[case] expected: bool,
    ) {
        assert_eq!(
            use_bounded_coalescing_gap(backend, source_sharing_configured),
            expected
        );
    }

    #[rstest]
    #[case(ScanBackend::V1, None, None, 1)]
    #[case(ScanBackend::Push, None, None, 1)]
    #[case(ScanBackend::PushFrontier, None, None, 1)]
    #[case(ScanBackend::V1, Some(7), Some(1), 1)]
    #[case(ScanBackend::Push, Some(7), Some(1), 1)]
    #[case(ScanBackend::PushFrontier, Some(7), Some(1), 1)]
    fn scan_concurrency_reaches_each_backend(
        #[case] backend: ScanBackend,
        #[case] requested: Option<usize>,
        #[case] expected_builder_concurrency: Option<usize>,
        #[case] expected_push_threads: usize,
    ) {
        assert_eq!(
            scan_execution_config(backend, requested),
            ScanExecutionConfig {
                builder_concurrency: expected_builder_concurrency,
                push_threads: expected_push_threads,
            }
        );
    }

    #[test]
    fn natural_split_diagnostics_use_fixed_handles_and_max_semantics() {
        let registry = DefaultMetricsRegistry::default();
        let diagnostics = NaturalSplitDiagnostics::new(&registry, &[]);
        diagnostics.record(
            false,
            Some(Duration::from_nanos(7)),
            Some(Duration::from_nanos(11)),
            Some(true),
        );
        diagnostics.record(true, Some(Duration::from_nanos(3)), None, None);
        diagnostics.record(false, None, Some(Duration::from_nanos(5)), Some(false));

        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), 9);
        let counter = |name: &str| {
            snapshot
                .iter()
                .find(|metric| metric.name().as_ref() == name)
                .map(|metric| match metric.value() {
                    MetricValue::Counter(counter) => counter.value(),
                    _ => 0,
                })
        };
        assert_eq!(counter("vortex.scan.natural_splits.calls"), Some(3));
        assert_eq!(
            counter("vortex.scan.natural_splits.lock_contended_acquisitions"),
            Some(2)
        );
        assert_eq!(
            counter("vortex.scan.natural_splits.successful_builds"),
            Some(1)
        );
        assert_eq!(counter("vortex.scan.natural_splits.failed_builds"), Some(1));
        let wait_max = snapshot
            .iter()
            .find(|metric| {
                metric.name().as_ref() == "vortex.scan.natural_splits.lock_wait_nanoseconds_max"
            })
            .map(|metric| match metric.value() {
                MetricValue::Gauge(gauge) => gauge.value(),
                _ => 0.0,
            });
        assert_eq!(wait_max, Some(7.0));
        let build_max = snapshot
            .iter()
            .find(|metric| {
                metric.name().as_ref() == "vortex.scan.natural_splits.build_nanoseconds_max"
            })
            .map(|metric| match metric.value() {
                MetricValue::Gauge(gauge) => gauge.value(),
                _ => 0.0,
            });
        assert_eq!(build_max, Some(11.0));
    }

    /// Test-only expr used to test error reporting.
    #[derive(Debug, Eq, Hash, PartialEq)]
    struct SnapshotErrorExpr;

    impl fmt::Display for SnapshotErrorExpr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "snapshot_error")
        }
    }

    impl PhysicalExpr for SnapshotErrorExpr {
        fn data_type(&self, _input_schema: &Schema) -> DFResult<DataType> {
            Ok(DataType::Boolean)
        }

        fn nullable(&self, _input_schema: &Schema) -> DFResult<bool> {
            Ok(false)
        }

        fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Display::fmt(self, f)
        }

        fn evaluate(&self, _batch: &RecordBatch) -> DFResult<datafusion_expr::ColumnarValue> {
            Err(DataFusionError::Internal(
                "intentional snapshot error".to_owned(),
            ))
        }

        fn children(&self) -> Vec<&PhysicalExprRef> {
            Vec::new()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<PhysicalExprRef>,
        ) -> DFResult<PhysicalExprRef> {
            assert!(children.is_empty());
            Ok(self)
        }

        fn snapshot(&self) -> DFResult<Option<PhysicalExprRef>> {
            Err(DataFusionError::Internal(
                "intentional snapshot error".to_owned(),
            ))
        }
    }

    fn natural_splits(total_size: u64, split_ranges: &[Range<u64>]) -> NaturalSplits {
        let mut row_boundaries = Vec::with_capacity(split_ranges.len() + 1);
        if let Some(first) = split_ranges.first() {
            row_boundaries.push(first.start);
            row_boundaries.extend(split_ranges.iter().map(|range| range.end));
        }
        NaturalSplits::new(row_boundaries.into(), total_size)
    }

    #[rstest]
    #[case(0..3, 10, vec![0..2, 2..5, 5..10], Some(0..2))]
    #[case(3..7, 10, vec![0..2, 2..5, 5..10], Some(2..5))]
    #[case(1..8, 10, vec![0..1, 1..9, 9..10], Some(1..9))]
    #[case(1..4, 16, vec![0..1, 1..2, 2..3, 3..4], None)]
    #[case(0..1, 10, vec![0..2, 2..10], Some(0..2))]
    #[case(0..2, 2, vec![], None)]
    fn test_split_aligned_row_range(
        #[case] byte_range: Range<u64>,
        #[case] total_size: u64,
        #[case] split_ranges: Vec<Range<u64>>,
        #[case] expected: Option<Range<u64>>,
    ) {
        assert_eq!(
            split_aligned_row_range(byte_range, &natural_splits(total_size, &split_ranges)),
            expected
        );
    }

    #[test]
    fn test_split_aligned_ranges_cover_splits_exactly_once() {
        let split_ranges = vec![0..1, 1..4, 4..10, 10..13];
        let byte_ranges = [0..4, 4..8, 8..12, 12..16];
        let natural_splits = natural_splits(16, &split_ranges);

        let assigned = byte_ranges
            .into_iter()
            .filter_map(|byte_range| split_aligned_row_range(byte_range, &natural_splits))
            .collect::<Vec<_>>();

        assert_eq!(assigned, vec![0..4, 4..10, 10..13]);
        assert_eq!(
            assigned
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>(),
            13
        );

        let split_starts = split_ranges
            .iter()
            .map(|range| range.start)
            .collect::<Vec<_>>();
        let split_ends = split_ranges
            .iter()
            .map(|range| range.end)
            .collect::<Vec<_>>();

        for range in &assigned {
            assert!(split_starts.contains(&range.start));
            assert!(split_ends.contains(&range.end));
        }

        for (left, right) in assigned.iter().tuple_windows() {
            assert_eq!(left.end, right.start);
        }
    }

    #[rstest]
    #[case(vec![], 10)]
    #[case(vec![0], 10)]
    #[case(vec![], 0)]
    #[case(vec![0], 0)]
    fn test_natural_splits_empty_file(#[case] row_boundaries: Vec<u64>, #[case] total_size: u64) {
        let splits = NaturalSplits::new(row_boundaries.clone().into(), total_size);

        assert!(splits.assignment_bytes.is_empty());
        assert_eq!(splits.row_boundaries.as_ref(), row_boundaries.as_slice());
        assert_eq!(split_aligned_row_range(0..u64::MAX, &splits), None);
    }

    #[test]
    fn test_split_aligned_row_range_keeps_colliding_assignments_together() {
        let natural_splits = natural_splits(2, &[0..1, 1..2, 2..3, 3..4]);

        assert_eq!(natural_splits.assignment_bytes.as_ref(), [0, 0, 1, 1]);
        assert_eq!(split_aligned_row_range(0..1, &natural_splits), Some(0..2));
        assert_eq!(split_aligned_row_range(1..2, &natural_splits), Some(2..4));
    }

    async fn write_arrow_to_vortex(
        object_store: Arc<dyn ObjectStore>,
        path: &str,
        rb: RecordBatch,
    ) -> anyhow::Result<u64> {
        let schema = rb.schema();
        let array = SESSION.arrow().from_arrow_record_batch(rb, &schema)?;
        let path = Path::parse(path)?;

        let mut write = ObjectStoreWrite::new(object_store, &path).await?;
        let flat: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
        let strategy = Arc::new(TableStrategy::new(Arc::clone(&flat), flat));
        let summary = SESSION
            .write_options()
            .with_strategy(strategy)
            .write(&mut write, array.to_array_stream())
            .await?;
        write.shutdown().await?;

        Ok(summary.size())
    }

    async fn scan_two_push_frontier_ranges(
        opener: &VortexOpener,
        metadata: &ObjectMeta,
    ) -> anyhow::Result<Vec<i32>> {
        let midpoint = metadata.size / 2;
        let mut left = PartitionedFile::new_with_range(
            metadata.location.to_string(),
            metadata.size,
            0,
            midpoint as i64,
        );
        left.object_meta = metadata.clone();
        let mut right = PartitionedFile::new_with_range(
            metadata.location.to_string(),
            metadata.size,
            midpoint as i64,
            metadata.size as i64,
        );
        right.object_meta = metadata.clone();

        let left = opener
            .open_with_backend(left, ScanBackend::PushFrontier)?
            .await?;
        let mut right_opener = opener.clone();
        right_opener.partition += 1;
        let right = right_opener
            .open_with_backend(right, ScanBackend::PushFrontier)?
            .await?;
        let (left, right) =
            futures::try_join!(left.try_collect::<Vec<_>>(), right.try_collect::<Vec<_>>())?;

        let mut values = left
            .into_iter()
            .chain(right)
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .vortex_expect("test file must contain Int32 values")
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        Ok(values)
    }

    fn make_opener(
        object_store: Arc<dyn ObjectStore>,
        table_schema: TableSchema,
        filter: Option<PhysicalExprRef>,
    ) -> VortexOpener {
        VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(object_store)),
            push_frontier_source_sharing: None,
            projection: ProjectionExprs::from_indices(&[0], table_schema.file_schema()),
            filter,
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema,
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            morsel_scan_metrics: Default::default(),
            natural_split_diagnostics: Default::default(),
            df_metrics: ExecutionPlanMetricsSet::new(),
            layout_readers: Default::default(),
            natural_splits: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            projection_pushdown: false,
            scan_concurrency: None,
        }
    }

    #[test]
    fn morsel_scan_metrics_are_shared_by_opener_clones() {
        let table_schema = TableSchema::from(Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )])));
        let opener = make_opener(Arc::new(InMemory::new()), table_schema.clone(), None);
        let opener_clone = opener.clone();
        let first = opener.morsel_scan_metrics();
        let cloned = opener_clone.morsel_scan_metrics();

        assert!(Arc::ptr_eq(&first, &cloned));

        let mut other_partition = make_opener(Arc::new(InMemory::new()), table_schema, None);
        other_partition.partition = 2;
        let other = other_partition.morsel_scan_metrics();
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[tokio::test]
    async fn test_open() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "part=1/file.vortex";
        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let data_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch.clone()).await?;

        let file_schema = batch.schema();
        let mut file = PartitionedFile::new(file_path.to_string(), data_size);
        file.partition_values = vec![ScalarValue::Int32(Some(1))];

        let table_schema = TableSchema::builder(Arc::clone(&file_schema))
            .with_table_partition_cols(vec![Arc::new(Field::new("part", DataType::Int32, false))])
            .build();

        // filter matches partition value
        let filter = col("part").eq(lit(1));
        let filter = logical2physical(&filter, table_schema.table_schema());

        let opener = make_opener(
            Arc::clone(&object_store),
            table_schema.clone(),
            Some(filter),
        );
        let stream = opener.open(file.clone()).unwrap().await.unwrap();

        let data = stream.try_collect::<Vec<_>>().await?;
        let num_batches = data.len();
        let num_rows = data.iter().map(|rb| rb.num_rows()).sum::<usize>();

        assert_eq!((num_batches, num_rows), (1, 3));

        // filter doesn't matches partition value
        let filter = col("part").eq(lit(2));
        let filter = logical2physical(&filter, table_schema.table_schema());

        let opener = make_opener(
            Arc::clone(&object_store),
            table_schema.clone(),
            Some(filter),
        );
        let stream = opener.open(file.clone()).unwrap().await.unwrap();

        let data = stream.try_collect::<Vec<_>>().await?;
        let num_batches = data.len();
        let num_rows = data.iter().map(|rb| rb.num_rows()).sum::<usize>();
        assert_eq!((num_batches, num_rows), (0, 0));

        Ok(())
    }

    #[tokio::test]
    async fn test_open_preserves_declared_schema_metadata() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "part=1/file.vortex";
        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)]))?;
        let data_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch.clone()).await?;

        let file_schema = Arc::new(
            batch.schema().as_ref().clone().with_metadata(
                [("table".to_string(), "metadata".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        let table_schema = TableSchema::builder(file_schema)
            .with_table_partition_cols(vec![Arc::new(
                Field::new("part", DataType::Int32, false).with_metadata(
                    [("partition".to_string(), "metadata".to_string())]
                        .into_iter()
                        .collect(),
                ),
            )])
            .build();
        let projection = ProjectionExprs::from_indices(&[0, 1], table_schema.table_schema());
        let expected_schema = Arc::new(projection.project_schema(table_schema.table_schema())?);

        assert_eq!(
            expected_schema.metadata().get("table"),
            Some(&"metadata".to_string())
        );
        assert_eq!(
            expected_schema.field(1).metadata().get("partition"),
            Some(&"metadata".to_string())
        );

        for projection_pushdown in [false, true] {
            let mut opener = make_opener(Arc::clone(&object_store), table_schema.clone(), None);
            opener.projection = projection.clone();
            opener.projection_pushdown = projection_pushdown;

            let mut file = PartitionedFile::new(file_path.to_string(), data_size);
            file.partition_values = vec![ScalarValue::Int32(Some(1))];
            let batches = opener.open(file)?.await?.try_collect::<Vec<_>>().await?;

            assert!(!batches.is_empty());
            for batch in batches {
                assert_eq!(batch.schema().as_ref(), expected_schema.as_ref());
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_open_all_valid_nullable_columns_with_nonnullable_table_schema()
    -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "nullable/file.vortex";
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
        )?;
        let data_size = write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch).await?;

        let expected_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let table_schema = TableSchema::from(Arc::clone(&expected_schema));

        for projection_pushdown in [false, true] {
            let mut opener = make_opener(Arc::clone(&object_store), table_schema.clone(), None);
            opener.projection_pushdown = projection_pushdown;

            let file = PartitionedFile::new(file_path.to_string(), data_size);
            let batches = opener.open(file)?.await?.try_collect::<Vec<_>>().await?;

            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].schema().as_ref(), expected_schema.as_ref());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_file_pruning_replaces_partition_columns_without_file_statistics()
    -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let table_schema = TableSchema::builder(Arc::clone(&file_schema))
            .with_table_partition_cols(vec![Arc::new(Field::new("part", DataType::Int32, false))])
            .build();

        let partition_column = Arc::new(df_expr::Column::new("part", 1)) as PhysicalExprRef;
        let predicate = Arc::new(df_expr::BinaryExpr::new(
            Arc::clone(&partition_column),
            Operator::Gt,
            df_expr::lit(ScalarValue::Int32(Some(1))),
        )) as PhysicalExprRef;
        let dynamic_predicate = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![partition_column],
            predicate,
        )) as PhysicalExprRef;

        let mut opener = make_opener(object_store, table_schema, None);
        opener.file_pruning_predicate = Some(dynamic_predicate);
        let df_metrics = opener.df_metrics.clone();

        // The file does not exist and has no statistics. Replacing `part` with 1
        // makes the predicate false, so pruning must happen before any file I/O.
        let mut file = PartitionedFile::new("missing.vortex", 1);
        file.partition_values = vec![ScalarValue::Int32(Some(1))];
        let batches = opener.open(file)?.await?.try_collect::<Vec<_>>().await?;

        assert!(batches.is_empty());
        assert_eq!(
            df_metrics
                .clone_inner()
                .sum_by_name("num_predicate_creation_errors")
                .map(|metric| metric.as_usize()),
            Some(0)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_file_pruning_creation_errors_are_reported() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "metrics/file.vortex";
        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let data_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch.clone()).await?;
        let mut statistics = Statistics::new_unknown(batch.schema().as_ref());
        statistics.column_statistics[0].null_count = Precision::Exact(0);
        let file = PartitionedFile::new(file_path, data_size).with_statistics(Arc::new(statistics));

        let mut opener = make_opener(object_store, TableSchema::from(batch.schema()), None);
        opener.file_pruning_predicate = Some(Arc::new(SnapshotErrorExpr));
        let df_metrics = opener.df_metrics.clone();

        let batches = opener.open(file)?.await?.try_collect::<Vec<_>>().await?;

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert_eq!(
            df_metrics
                .clone_inner()
                .sum_by_name("num_predicate_creation_errors")
                .map(|metric| metric.as_usize()),
            Some(1)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_open_empty_file() -> anyhow::Result<()> {
        use futures::TryStreamExt;

        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let data_batch = record_batch!(("a", Int32, Vec::<i32>::new())).unwrap();
        let file_path = "part=1/empty.vortex";
        let file_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, data_batch.clone()).await?;

        let file_schema = data_batch.schema();
        // Parallel scans may attach a byte range even for empty files; the
        // opener must return early before attempting split-aligned translation.
        let file =
            PartitionedFile::new_with_range(file_path.to_string(), file_size, 0, file_size as i64);

        let table_schema = TableSchema::from(Arc::clone(&file_schema));

        let opener = make_opener(object_store, table_schema, None);
        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;

        assert_eq!(data.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_open_populates_file_metadata_cache() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "cached/file.vortex";
        let batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
        let data_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch.clone()).await?;

        let file = PartitionedFile::new(file_path.to_string(), data_size);
        let table_schema = TableSchema::from(batch.schema());

        let cache: Arc<FileMetadataCache> = Arc::new(
            DefaultCache::<Path, CachedFileMetadataEntry>::new(64 * 1024 * 1024),
        );
        let mut opener = make_opener(Arc::clone(&object_store), table_schema, None);
        opener.file_metadata_cache = Some(Arc::clone(&cache));

        // The first open misses the cache and must write the parsed footer back.
        let stream = opener.open(file.clone())?.await?;
        stream.try_collect::<Vec<_>>().await?;

        let entry = cache
            .get(file.path())
            .ok_or_else(|| anyhow::anyhow!("footer was not cached after open"))?;
        assert!(entry.is_valid_for(&file.object_meta));
        assert!(
            entry
                .file_metadata
                .as_any()
                .downcast_ref::<CachedVortexMetadata>()
                .is_some()
        );

        // The second open hits the cache and still returns the same data.
        let stream = opener.open(file.clone())?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;
        assert_eq!(data.iter().map(|rb| rb.num_rows()).sum::<usize>(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn push_frontier_isolates_same_path_object_versions_end_to_end() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "versioned/file.vortex";
        let first_batch = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3), Some(4)]))?;
        let first_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, first_batch.clone())
                .await?;
        let fixed_timestamp = "2026-09-12T12:34:56.123456789Z".parse()?;
        let first_metadata = ObjectMeta {
            location: Path::from(file_path),
            last_modified: fixed_timestamp,
            size: first_size,
            e_tag: Some("etag-v1".to_owned()),
            version: Some("version-v1".to_owned()),
        };

        let cache: Arc<FileMetadataCache> = Arc::new(
            DefaultCache::<Path, CachedFileMetadataEntry>::new(64 * 1024 * 1024),
        );
        let pool = Arc::new(PushFrontierSegmentSourcePool::default());
        let mut opener = make_opener(
            Arc::clone(&object_store),
            TableSchema::from(first_batch.schema()),
            None,
        );
        opener.file_metadata_cache = Some(Arc::clone(&cache));
        opener.push_frontier_source_sharing = Some(PushFrontierSourceSharing::new(
            Arc::clone(&pool),
            &object_store,
            2,
        ));

        assert_eq!(
            scan_two_push_frontier_ranges(&opener, &first_metadata).await?,
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            cache
                .get(&first_metadata.location)
                .vortex_expect("first footer must be cached")
                .meta,
            first_metadata
        );

        let second_batch = record_batch!(("a", Int32, vec![Some(5), Some(6), Some(7), Some(8)]))?;
        let second_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, second_batch).await?;
        assert_eq!(
            second_size, first_size,
            "test versions must have equal size"
        );
        let second_metadata = ObjectMeta {
            e_tag: Some("etag-v2".to_owned()),
            version: Some("version-v2".to_owned()),
            ..first_metadata.clone()
        };

        assert_eq!(
            scan_two_push_frontier_ranges(&opener, &second_metadata).await?,
            vec![5, 6, 7, 8]
        );
        assert_eq!(
            cache
                .get(&second_metadata.location)
                .vortex_expect("second footer must replace the stale version")
                .meta,
            second_metadata
        );

        let physical_metrics = opener
            .metrics_registry
            .snapshot()
            .into_iter()
            .filter(|metric| {
                metric.name().starts_with("vortex.io.read.")
                    || metric.name().starts_with("io.read_ranges.")
                    || metric.name().starts_with("io.requests.")
            })
            .collect::<Vec<_>>();
        assert!(!physical_metrics.is_empty());
        assert!(physical_metrics.iter().all(|metric| {
            metric.labels().iter().any(|label| {
                label.key() == PATH_LABEL && label.value() == first_metadata.location.as_ref()
            }) && metric
                .labels()
                .iter()
                .all(|label| label.key() != PARTITION_LABEL)
        }));
        let mut label_sets = physical_metrics
            .iter()
            .map(|metric| {
                metric
                    .labels()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        label_sets.sort();
        label_sets.dedup();
        assert_eq!(label_sets.len(), 1);
        assert!(pool.len() <= 2);
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn test_open_files_different_table_schema() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let file1 = {
            let file1_path = "/path/file1.vortex";
            let batch1 = record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3)])).unwrap();
            let data_size1 =
                write_arrow_to_vortex(Arc::clone(&object_store), file1_path, batch1).await?;
            PartitionedFile::new(file1_path.to_string(), data_size1)
        };

        let file2 = {
            let file2_path = "/path/file2.vortex";
            let batch2 = record_batch!(("a", Int16, vec![Some(-1), Some(-2), Some(-3)])).unwrap();
            let data_size2 =
                write_arrow_to_vortex(Arc::clone(&object_store), file2_path, batch2).await?;
            PartitionedFile::new(file2_path.to_string(), data_size2)
        };

        // Table schema has can accommodate both files
        let table_schema = TableSchema::from(Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            true,
        )])));

        let make_opener = |filter| VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(Arc::clone(
                &object_store,
            ))),
            push_frontier_source_sharing: None,
            projection: ProjectionExprs::from_indices(&[0], table_schema.file_schema()),
            filter: Some(filter),
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema: table_schema.clone(),
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            morsel_scan_metrics: Default::default(),
            natural_split_diagnostics: Default::default(),
            df_metrics: ExecutionPlanMetricsSet::new(),
            layout_readers: Default::default(),
            natural_splits: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            projection_pushdown: false,
            scan_concurrency: None,
        };

        let filter = col("a").lt(lit(100_i32));
        let filter = logical2physical(&filter, table_schema.table_schema());

        let opener1 = make_opener(Arc::clone(&filter));
        let stream = opener1.open(file1)?.await?;

        let format_opts = FormatOptions::new().with_types_info(true);

        let data = stream.try_collect::<Vec<_>>().await?;
        assert_snapshot!(pretty_format_batches_with_options(&data, &format_opts)?.to_string(), @r"
        +-------+
        | a     |
        | Int32 |
        +-------+
        | 1     |
        | 2     |
        | 3     |
        +-------+
        ");

        let opener2 = make_opener(Arc::clone(&filter));
        let stream = opener2.open(file2)?.await?;

        let data = stream.try_collect::<Vec<_>>().await?;
        assert_snapshot!(pretty_format_batches_with_options(&data, &format_opts)?.to_string(), @r"
        +-------+
        | a     |
        | Int32 |
        +-------+
        | -1    |
        | -2    |
        | -3    |
        +-------+
        ");

        Ok(())
    }

    #[tokio::test]
    // This test verifies that files with different column order than the
    // table schema can be opened without errors. The fix ensures that the
    // schema mapper is only used for type casting, not for reordering,
    // since the vortex projection already handles reordering.
    async fn test_schema_different_column_order() -> anyhow::Result<()> {
        use datafusion::arrow::util::pretty::pretty_format_batches_with_options;

        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        // File has columns in order: c, b, a
        let batch = record_batch!(
            ("c", Int32, vec![Some(300), Some(301), Some(302)]),
            ("b", Int32, vec![Some(200), Some(201), Some(202)]),
            ("a", Int32, vec![Some(100), Some(101), Some(102)])
        )
        .unwrap();
        let data_size = write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch).await?;
        let file = PartitionedFile::new(file_path.to_string(), data_size);

        // Table schema has columns in different order: a, b, c
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]));

        let opener = VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(object_store)),
            push_frontier_source_sharing: None,
            projection: ProjectionExprs::from_indices(&[0, 1, 2], &table_schema),
            filter: None,
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema: TableSchema::from(Arc::clone(&table_schema)),
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            morsel_scan_metrics: Default::default(),
            natural_split_diagnostics: Default::default(),
            df_metrics: ExecutionPlanMetricsSet::new(),
            layout_readers: Default::default(),
            natural_splits: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            projection_pushdown: false,
            scan_concurrency: None,
        };

        let stream = opener.open(file)?.await?;

        let format_opts = FormatOptions::new().with_types_info(true);
        let data = stream.try_collect::<Vec<_>>().await?;

        // Verify the output has columns in table schema order (a, b, c)
        // not file order (c, b, a)
        assert_snapshot!(pretty_format_batches_with_options(&data, &format_opts)?.to_string(), @r"
        +-------+-------+-------+
        | a     | b     | c     |
        | Int32 | Int32 | Int32 |
        +-------+-------+-------+
        | 100   | 200   | 300   |
        | 101   | 201   | 301   |
        | 102   | 202   | 302   |
        +-------+-------+-------+
        ");

        Ok(())
    }

    #[tokio::test]
    #[ignore = "the CI-only morsel executor does not support nested struct layouts"]
    // This test verifies that expression rewriting doesn't fail when there is
    // a nested schema mismatch between the physical file schema and logical
    // table schema.
    async fn test_adapter_logical_physical_struct_mismatch() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";
        let file_struct_fields = Fields::from(vec![
            Field::new("field1", DataType::Utf8, true),
            Field::new("field2", DataType::Utf8, true),
        ]);
        let struct_array = StructArray::new(
            file_struct_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["value1", "value2", "value3"])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "my_struct",
                DataType::Struct(file_struct_fields),
                true,
            )])),
            vec![Arc::new(struct_array)],
        )?;
        let data_size = write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch).await?;

        // Table schema has an extra utf8 field.
        let table_schema = TableSchema::from(Arc::new(Schema::new(vec![Field::new(
            "my_struct",
            DataType::Struct(Fields::from(vec![
                Field::new(
                    "field1",
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                    true,
                ),
                Field::new(
                    "field2",
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                    true,
                ),
                Field::new("field3", DataType::Utf8, true),
            ])),
            true,
        )])));

        let opener = make_opener(
            Arc::clone(&object_store),
            table_schema.clone(),
            // expression references my_struct column which has different fields in each
            // field.
            Some(logical2physical(
                &col("my_struct").is_not_null(),
                table_schema.table_schema(),
            )),
        );

        // The opener should be able to open the file with a filter on the
        // struct column.
        let data = opener
            .open(PartitionedFile::new(file_path.to_string(), data_size))?
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].num_rows(), 3);

        Ok(())
    }

    #[tokio::test]
    // Minimal reproducing test for the schema projection bug.
    // Before the fix, this would fail with a cast error when the file schema
    // and table schema have different field orders and we project a subset of columns.
    async fn test_projection_bug_minimal_repro() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        // File has columns in order: a, b, c with simple types
        let batch = record_batch!(
            ("a", Int32, vec![Some(1)]),
            ("b", Utf8, vec![Some("test")]),
            ("c", Int32, vec![Some(2)])
        )
        .unwrap();
        let data_size = write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch).await?;

        // Table schema has columns in DIFFERENT order: c, a, b
        // and different types that require casting (Utf8 -> Dictionary)
        let table_schema = TableSchema::from(Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
            Field::new(
                "b",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                true,
            ),
        ])));

        // Project columns [0, 2] from table schema, which should give us: c, b
        // Before the fix, the schema adapter would get confused about which fields
        // to select from the file, causing incorrect type mappings.
        let projection = vec![0, 2];

        let opener = VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(Arc::clone(
                &object_store,
            ))),
            push_frontier_source_sharing: None,
            projection: ProjectionExprs::from_indices(
                projection.as_ref(),
                table_schema.file_schema(),
            ),
            filter: None,
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema: table_schema.clone(),
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            morsel_scan_metrics: Default::default(),
            natural_split_diagnostics: Default::default(),
            df_metrics: ExecutionPlanMetricsSet::new(),
            layout_readers: Default::default(),
            natural_splits: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            projection_pushdown: false,
            scan_concurrency: None,
        };

        // This should succeed and return the correctly projected and cast data
        let data = opener
            .open(PartitionedFile::new(file_path.to_string(), data_size))?
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        // Verify the columns are in the right order and have the right values
        use datafusion::arrow::util::pretty::pretty_format_batches_with_options;
        let format_opts = FormatOptions::new().with_types_info(true);
        assert_snapshot!(pretty_format_batches_with_options(&data, &format_opts)?.to_string(), @r"
        +-------+--------------------------+
        | c     | b                        |
        | Int32 | Dictionary(UInt32, Utf8) |
        +-------+--------------------------+
        | 2     | test                     |
        +-------+--------------------------+
        ");

        Ok(())
    }

    fn make_test_batch_with_10_rows() -> RecordBatch {
        record_batch!(
            ("a", Int32, (0..=9).map(Some).collect::<Vec<_>>()),
            (
                "b",
                Utf8,
                (0..=9).map(|i| Some(format!("r{}", i))).collect::<Vec<_>>()
            )
        )
        .unwrap()
    }

    fn make_test_opener(
        object_store: Arc<dyn ObjectStore>,
        schema: SchemaRef,
        projection: ProjectionExprs,
    ) -> VortexOpener {
        VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(object_store)),
            push_frontier_source_sharing: None,
            projection,
            filter: None,
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema: TableSchema::from(schema),
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            morsel_scan_metrics: Default::default(),
            natural_split_diagnostics: Default::default(),
            df_metrics: ExecutionPlanMetricsSet::new(),
            layout_readers: Default::default(),
            natural_splits: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            projection_pushdown: false,
            scan_concurrency: None,
        }
    }

    #[tokio::test]
    // Test that Selection::IncludeByIndex filters to specific row indices.
    async fn test_selection_include_by_index() -> anyhow::Result<()> {
        use datafusion::arrow::util::pretty::pretty_format_batches_with_options;

        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        let batch = make_test_batch_with_10_rows();
        let data_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch.clone()).await?;

        let schema = batch.schema();
        let mut file = PartitionedFile::new(file_path.to_string(), data_size);
        file.extensions
            .insert(
                VortexAccessPlan::default().with_selection(Selection::IncludeByIndex(
                    StrictSortedBuffer::try_new(Buffer::from_iter(vec![1, 3, 5, 7]))?,
                )),
            );

        let opener = make_test_opener(
            Arc::clone(&object_store),
            Arc::clone(&schema),
            ProjectionExprs::from_indices(&[0, 1], &schema),
        );

        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;
        let format_opts = FormatOptions::new().with_types_info(true);

        assert_snapshot!(pretty_format_batches_with_options(&data, &format_opts)?.to_string(), @r"
        +-------+------+
        | a     | b    |
        | Int32 | Utf8 |
        +-------+------+
        | 1     | r1   |
        | 3     | r3   |
        | 5     | r5   |
        | 7     | r7   |
        +-------+------+
        ");

        Ok(())
    }

    #[tokio::test]
    // Test that Selection::ExcludeByIndex excludes specific row indices.
    async fn test_selection_exclude_by_index() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        let batch = make_test_batch_with_10_rows();
        let data_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch.clone()).await?;

        let schema = batch.schema();
        let mut file = PartitionedFile::new(file_path.to_string(), data_size);
        file.extensions
            .insert(
                VortexAccessPlan::default().with_selection(Selection::ExcludeByIndex(
                    StrictSortedBuffer::try_new(Buffer::from_iter(vec![0, 2, 4, 6, 8]))?,
                )),
            );

        let opener = make_test_opener(
            Arc::clone(&object_store),
            Arc::clone(&schema),
            ProjectionExprs::from_indices(&[0, 1], &schema),
        );

        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;
        let format_opts = FormatOptions::new().with_types_info(true);

        assert_snapshot!(pretty_format_batches_with_options(&data, &format_opts)?.to_string(), @r"
        +-------+------+
        | a     | b    |
        | Int32 | Utf8 |
        +-------+------+
        | 1     | r1   |
        | 3     | r3   |
        | 5     | r5   |
        | 7     | r7   |
        | 9     | r9   |
        +-------+------+
        ");

        Ok(())
    }

    #[tokio::test]
    // Test that Selection::All returns all rows.
    async fn test_selection_all() -> anyhow::Result<()> {
        use vortex::scan::selection::Selection;

        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        let batch = make_test_batch_with_10_rows();
        let data_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch.clone()).await?;

        let schema = batch.schema();
        let mut file = PartitionedFile::new(file_path.to_string(), data_size);
        file.extensions
            .insert(VortexAccessPlan::default().with_selection(Selection::All));

        let opener = make_test_opener(
            Arc::clone(&object_store),
            Arc::clone(&schema),
            ProjectionExprs::from_indices(&[0], &schema),
        );

        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;

        let total_rows: usize = data.iter().map(|rb| rb.num_rows()).sum();
        assert_eq!(total_rows, 10);

        Ok(())
    }

    #[tokio::test]
    // Test that when no extensions are provided, all rows are returned (backward compatibility).
    async fn test_selection_no_extensions() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        let batch = make_test_batch_with_10_rows();
        let data_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch.clone()).await?;

        let schema = batch.schema();
        let file = PartitionedFile::new(file_path.to_string(), data_size);
        // file.extensions is None by default

        let opener = make_test_opener(
            Arc::clone(&object_store),
            Arc::clone(&schema),
            ProjectionExprs::from_indices(&[0], &schema),
        );

        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;

        let total_rows: usize = data.iter().map(|rb| rb.num_rows()).sum();
        assert_eq!(total_rows, 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_projection_expr_pushdown() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let file_path = "/path/file.vortex";

        let batch = record_batch!(
            ("a", Int32, vec![Some(1), Some(2), Some(3)]),
            ("b", Int32, vec![Some(10), Some(20), Some(30)])
        )
        .unwrap();
        let data_size =
            write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch.clone()).await?;

        let file_schema = batch.schema();
        let table_schema = TableSchema::from(Arc::clone(&file_schema));

        // Create a projection that includes an arithmetic expression: a + b * 2
        let col_a = df_expr::col("a", &file_schema)?;
        let col_b = df_expr::col("b", &file_schema)?;
        let two = df_expr::lit(ScalarValue::Int32(Some(2)));

        // b * 2
        let b_times_2 = df_expr::binary(col_b, Operator::Multiply, two, &file_schema)?;
        // a + (b * 2)
        let a_plus_b_times_2 = df_expr::binary(col_a, Operator::Plus, b_times_2, &file_schema)?;

        let projection = ProjectionExprs::new(vec![ProjectionExpr::new(
            a_plus_b_times_2,
            "result".to_string(),
        )]);

        let opener = VortexOpener {
            partition: 1,
            session: SESSION.clone(),
            vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(Arc::clone(
                &object_store,
            ))),
            push_frontier_source_sharing: None,
            projection,
            filter: None,
            file_pruning_predicate: None,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
            table_schema,
            limit: None,
            metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
            morsel_scan_metrics: Default::default(),
            natural_split_diagnostics: Default::default(),
            df_metrics: ExecutionPlanMetricsSet::new(),
            layout_readers: Default::default(),
            natural_splits: Default::default(),
            has_output_ordering: false,
            expression_convertor: Arc::new(DefaultExpressionConvertor::default()),
            file_metadata_cache: None,
            projection_pushdown: false,
            scan_concurrency: None,
        };

        let file = PartitionedFile::new(file_path.to_string(), data_size);
        let stream = opener.open(file)?.await?;
        let data = stream.try_collect::<Vec<_>>().await?;

        // Expected: a + b * 2
        // row 0: 1 + 10 * 2 = 21
        // row 1: 2 + 20 * 2 = 42
        // row 2: 3 + 30 * 2 = 63
        assert_snapshot!(pretty_format_batches_with_options(&data, &FormatOptions::new().with_types_info(true))?.to_string(), @r"
        +--------+
        | result |
        | Int32  |
        +--------+
        | 21     |
        | 42     |
        | 63     |
        +--------+
        ");

        Ok(())
    }

    /// When a Struct contains Dictionary fields, writing to vortex and reading back
    /// should preserve the Dictionary type.
    #[tokio::test]
    #[ignore = "the CI-only morsel executor does not support dictionary layouts"]
    async fn test_struct_with_dictionary_roundtrip() -> anyhow::Result<()> {
        let object_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;

        let struct_fields = Fields::from(vec![
            Field::new_dictionary("a", DataType::UInt32, DataType::Utf8, true),
            Field::new_dictionary("b", DataType::UInt32, DataType::Utf8, true),
        ]);
        let struct_array = StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(DictionaryArray::<UInt32Type>::from_iter(["x", "y", "x"])),
                Arc::new(DictionaryArray::<UInt32Type>::from_iter(["p", "p", "q"])),
            ],
            None,
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "labels",
            DataType::Struct(struct_fields.clone()),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(struct_array)])?;

        let file_path = "/test.vortex";
        let data_size = write_arrow_to_vortex(Arc::clone(&object_store), file_path, batch).await?;

        let opener = make_test_opener(
            Arc::clone(&object_store),
            Arc::clone(&schema),
            ProjectionExprs::from_indices(&[0], &schema),
        );
        let data: Vec<_> = opener
            .open(PartitionedFile::new(file_path.to_string(), data_size))?
            .await?
            .try_collect()
            .await?;

        assert_eq!(
            data[0].schema().field(0).data_type(),
            &DataType::Struct(struct_fields),
            "Struct(Dictionary) type should be preserved"
        );
        Ok(())
    }
}
