// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::future::ready;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Weak;

use arrow_array::RecordBatchOptions;
use arrow_schema::Field;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_common::Statistics;
use datafusion_common::arrow::array::AsArray;
use datafusion_common::arrow::array::RecordBatch;
use datafusion_common::exec_datafusion_err;
use datafusion_common::tree_node::Transformed;
use datafusion_common::tree_node::TreeNode;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::TableSchema;
use datafusion_datasource::file_stream::FileOpenFuture;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_execution::cache::cache_manager::CachedFileMetadataEntry;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions as df_expr;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr::split_conjunction;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::utils::conjunction;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use datafusion_physical_plan::filter::batch_filter;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::metrics::MetricBuilder;
use datafusion_physical_plan::metrics::MetricCategory;
use datafusion_pruning::FilePruner;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use object_store::path::Path;
use tracing::Instrument;
use vortex::array::VortexSessionExecute;
use vortex::error::VortexError;
use vortex::error::VortexExpect;
use vortex::file::OpenOptionsSessionExt;
use vortex::io::InstrumentedReadAt;
use vortex::layout::LayoutReader;
use vortex::layout::scan::scan_builder::ScanBuilder;
use vortex::metrics::Label;
use vortex::metrics::MetricsRegistry;
use vortex::session::VortexSession;
use vortex_arrow::ArrowSessionExt;
use vortex_utils::aliases::dash_map::DashMap;
use vortex_utils::aliases::dash_map::Entry;

use crate::VortexAccessPlan;
use crate::convert::exprs::ExpressionConvertor;
use crate::convert::exprs::ProcessedProjection;
use crate::convert::exprs::raw_projection;
use crate::convert::schema::calculate_physical_schema;
use crate::metrics::PARTITION_LABEL;
use crate::metrics::PATH_LABEL;
use crate::persistent::cache::CachedVortexMetadata;
use crate::persistent::reader::VortexReaderFactory;
use crate::persistent::stream::PrunableStream;

#[derive(Clone)]
pub(crate) struct VortexOpener {
    /// The partition this opener is assigned to. Only used for labeling metrics.
    pub partition: usize,
    pub session: VortexSession,
    pub vortex_reader_factory: Arc<dyn VortexReaderFactory>,
    /// Optional table schema projection. The indices are w.r.t. the `table_schema`, which is
    /// all fields in the final scan result not including the partition columns.
    pub projection: ProjectionExprs,
    /// Exact filter accepted during planning. Per-file adaptation may move parts
    /// of it to DataFusion residual evaluation before projection and limits.
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

impl FileOpener for VortexOpener {
    fn open(&self, file: PartitionedFile) -> DFResult<FileOpenFuture> {
        // Calculate the output schema before replacing partition columns with literals so it
        // retains the table and partition-field metadata declared by the plan.
        let output_schema = Arc::new(
            self.projection
                .project_schema(self.table_schema.table_schema())?,
        );
        let session = self.session.clone();
        let metrics_registry = Arc::clone(&self.metrics_registry);
        let labels = vec![
            Label::new(PATH_LABEL, file.path().to_string()),
            Label::new(PARTITION_LABEL, self.partition.to_string()),
        ];

        let mut projection = self.projection.clone();
        let mut filter = self.filter.clone();

        let reader = self.vortex_reader_factory.create_reader(&file, &session)?;

        let reader =
            InstrumentedReadAt::new_with_labels(reader, metrics_registry.as_ref(), labels.clone());

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
                .with_labels(labels);

            let cached_footer = file_metadata_cache
                .as_ref()
                .and_then(|cache| cache.get(file.path()))
                .filter(|entry| entry.is_valid_for(&file.object_meta))
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
                    let adapted = expr_adapter.rewrite(Arc::clone(&filter))
                        .map_err(|e| exec_datafusion_err!("Failed to adapt filter {filter} in {}: {e}", file.path()))?;
                    simplifier.simplify(adapted)
                        .map_err(|e| exec_datafusion_err!("Failed to simplify filter {filter} in {}: {e}", file.path()))
                })
                .transpose()?;
            let projection =
                projection.try_map_exprs(|p| simplifier.simplify(expr_adapter.rewrite(p)?))?;

            let mut native_filters = Vec::new();
            let mut residual_filters = Vec::new();
            if let Some(filter) = &filter {
                if filter.data_type(&this_file_schema)? != arrow_schema::DataType::Boolean {
                    return Err(exec_datafusion_err!("Filter must be Boolean in {}: {filter}", file.path()));
                }
                for expr in split_conjunction(filter) {
                    match expr_convertor.try_convert(expr, &this_file_schema)
                        .map_err(|e| exec_datafusion_err!("Failed to convert filter {expr} in {}: {e}", file.path()))?
                    {
                        Some(expr) => native_filters.push(expr),
                        None => residual_filters.push(Arc::clone(expr)),
                    }
                }
            }
            let residual_filter = if residual_filters.is_empty() {
                None
            } else {
                Some(conjunction(residual_filters))
            };
            let native_filter = vortex::expr::and_collect(native_filters)
                .map(|filter| filter.optimize_recursive(vxf.dtype())?.bind(vxf.dtype()))
                .transpose()
                .map_err(|e| exec_datafusion_err!("Couldn't bind Vortex scan filter in {}: {e}", file.path()))?;

            // Residual filters must see raw inputs before computed projections or aliases.
            let mut residual_columns = None;
            let ProcessedProjection {
                scan_projection,
                scan_reference_schema,
                leftover_projection,
            } = if let Some(residual) = &residual_filter {
                let mut indices = projection.column_indices();
                indices.extend(collect_columns(residual).into_iter().map(|column| column.index()));
                indices.sort_unstable();
                indices.dedup();
                let required = ProjectionExprs::from_indices(&indices, &this_file_schema);
                let raw = raw_projection(required, &this_file_schema)?;
                residual_columns = Some(indices);
                ProcessedProjection {
                    scan_projection: raw.scan_projection,
                    scan_reference_schema: raw.scan_reference_schema,
                    leftover_projection: projection,
                }
            } else if projection_pushdown {
                expr_convertor.split_projection(
                    projection,
                    &this_file_schema,
                    output_schema.as_ref(),
                )?
            } else {
                expr_convertor.no_pushdown_projection(projection, &this_file_schema)?
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

            let stream_schema =
                calculate_physical_schema(&scan_dtype, &scan_reference_schema, &session.arrow())?;

            let (leftover_projection, residual_filter) = if let Some(indices) = residual_columns {
                (
                    leftover_projection.try_map_exprs(|expr| reassign_raw_columns(expr, &indices))?,
                    residual_filter.map(|expr| reassign_raw_columns(expr, &indices)).transpose()?,
                )
            } else {
                (
                    leftover_projection.try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?,
                    residual_filter,
                )
            };
            let projector = leftover_projection.make_projector(&stream_schema)?;

            // We share our layout readers with others partitions in the scan, so we can only need to read each layout in each file once.
            let layout_reader = match layout_readers.entry(file.object_meta.location.clone()) {
                Entry::Occupied(mut occupied_entry) => {
                    if let Some(reader) = occupied_entry.get().upgrade() {
                        tracing::trace!("reusing layout reader for {}", occupied_entry.key());
                        reader
                    } else {
                        tracing::trace!("creating layout reader for {}", occupied_entry.key());
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
                        DataFusionError::Execution(format!("Failed to create layout reader: {e}"))
                    })?;
                    vacant_entry.insert(Arc::downgrade(&reader));

                    reader
                }
            };

            let mut scan_builder = ScanBuilder::new(session.clone(), Arc::clone(&layout_reader));

            if let Some(vortex_plan) = file.extensions.get::<VortexAccessPlan>() {
                scan_builder = vortex_plan.apply_to_builder(scan_builder);
            }

            if let Some(limit) = limit
                && native_filter.is_none()
                && residual_filter.is_none()
            {
                scan_builder = scan_builder.with_limit(limit);
            }

            if let Some(concurrency) = scan_concurrency {
                scan_builder = scan_builder.with_concurrency(concurrency);
            }

            // Set before the byte-range translation below, which computes natural splits for
            // the fields the scan's projection and filter reference.
            scan_builder = scan_builder
                .with_projection(scan_projection)
                .with_some_filter(native_filter);

            if let Some(file_range) = &file.range {
                let byte_range = Range {
                    start: u64::try_from(file_range.start)
                        .map_err(|_| exec_datafusion_err!("Vortex file range start is negative"))?,
                    end: u64::try_from(file_range.end)
                        .map_err(|_| exec_datafusion_err!("Vortex file range end is negative"))?,
                };
                if byte_range.start != 0 || byte_range.end != file.object_meta.size {
                    // Full-file scans already cover every natural split. Only translate the
                    // byte range back into row boundaries when DataFusion has trimmed the file.
                    let natural_splits = natural_splits_for_file(
                        natural_splits.as_ref(),
                        &file.object_meta.location,
                        &scan_builder,
                        file.object_meta.size,
                    )?;

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
            let residual_path = file.path().clone();
            let stream = scan_builder
                .with_metrics_registry(metrics_registry)
                .with_ordered(has_output_ordering)
                .map(move |chunk| {
                    let mut ctx = session.create_execution_ctx();
                    let arrow_session = ctx.session().clone();
                    let arrow = arrow_session.arrow().execute_arrow(
                        chunk,
                        Some(&stream_target_field),
                        &mut ctx,
                    )?;
                    Ok(RecordBatch::from(arrow.as_struct().clone()))
                })
                .into_stream()
                .map_err(|e| exec_datafusion_err!("Failed to create Vortex stream: {e}"))?
                .map_err(move |e: VortexError| {
                    DataFusionError::External(Box::new(e.with_context(format!(
                        "Failed to read Vortex file: {}",
                        file.object_meta.location
                    ))))
                })
                .map(move |batch| -> DFResult<RecordBatch> {
                    let mut batch = batch?;
                    if let Some(residual) = &residual_filter {
                        batch = batch_filter(&batch, residual)
                            .map_err(|e| exec_datafusion_err!("Failed to evaluate residual filter {residual} in {residual_path}: {e}"))?;
                    }
                    Ok(batch)
                })
                .try_filter(|batch| ready(batch.num_rows() != 0))
                .map(move |batch| {
                    let batch = projector.project_batch(&batch?)?;

                    let (_, columns, row_count) = batch.into_parts();
                    RecordBatch::try_new_with_options(
                        Arc::clone(&output_schema),
                        columns,
                        &RecordBatchOptions::new().with_row_count(Some(row_count)),
                    )
                    .map_err(Into::into)
                })
                .boxed();

            if let Some(file_pruner) = file_pruner && file_pruner.is_watching() {
                Ok(PrunableStream::new(file_pruner, stream).boxed())
            } else {
                Ok(stream)
            }
        }
        .in_current_span()
        .boxed())
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
    /// boundaries back to the scan via [`ScanBuilder::with_natural_splits`], skipping the
    /// per-partition layout walk in `prepare`.
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

/// Return the cached [`NaturalSplits`] for `path`, computing and caching them on first use.
fn natural_splits_for_file<A: 'static + Send>(
    natural_splits: &DashMap<Path, Arc<NaturalSplits>>,
    path: &Path,
    scan_builder: &ScanBuilder<A>,
    total_size: u64,
) -> DFResult<Arc<NaturalSplits>> {
    if let Some(splits) = natural_splits.get(path) {
        return Ok(Arc::clone(splits.value()));
    }

    // Compute while holding the entry so concurrent partitions opening the same file wait
    // for the winner instead of all walking the layout tree; the redundant walks contend on
    // the lazily-initialized layout children and dominate the cost of the computation itself.
    match natural_splits.entry(path.clone()) {
        Entry::Occupied(entry) => Ok(Arc::clone(entry.get())),
        Entry::Vacant(entry) => {
            let splits = compute_natural_splits(scan_builder, total_size)?;
            entry.insert(Arc::clone(&splits));
            Ok(splits)
        }
    }
}

/// Walk the layout tree to compute the file's full natural split boundaries for the fields
/// referenced by the scan's projection and filter.
fn compute_natural_splits<A: 'static + Send>(
    scan_builder: &ScanBuilder<A>,
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

/// Remap physical file indices onto the ordered raw columns emitted by the scan.
fn reassign_raw_columns(expr: PhysicalExprRef, indices: &[usize]) -> DFResult<PhysicalExprRef> {
    expr.transform_up(|expr| {
        let Some(column) = expr.downcast_ref::<df_expr::Column>() else {
            return Ok(Transformed::no(expr));
        };
        let index = indices
            .binary_search(&column.index())
            .map_err(|_| exec_datafusion_err!("Missing raw filter/projection column {column}"))?;
        Ok(Transformed::yes(
            Arc::new(df_expr::Column::new(column.name(), index)) as PhysicalExprRef,
        ))
    })
    .map(|result| result.data)
}

#[cfg(test)]
mod tests;
