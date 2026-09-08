// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

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
use datafusion_common::arrow::compute::concat_batches;
use datafusion_common::assert_batches_eq;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::Transformed;
use datafusion_common::tree_node::TreeNode;
use datafusion_execution::cache::default_cache::DefaultCache;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions as df_expr;
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion_physical_expr::projection::ProjectionExpr;
use insta::assert_snapshot;
use itertools::Itertools;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use rstest::rstest;
use vortex::VortexSessionDefault;
use vortex::buffer::Buffer;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::VortexWrite;
use vortex::io::object_store::ObjectStoreWrite;
use vortex::metrics::DefaultMetricsRegistry;
use vortex::scan::selection::Selection;
use vortex::scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex::session::VortexSession;

use super::*;
use crate::VortexAccessPlan;
use crate::convert::exprs::DefaultExpressionConvertor;
use crate::persistent::reader::DefaultVortexReaderFactory;

static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::default);

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
    let summary = SESSION
        .write_options()
        .write(&mut write, array.to_array_stream())
        .await?;
    write.shutdown().await?;

    Ok(summary.size())
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
        projection: ProjectionExprs::from_indices(&[0], table_schema.file_schema()),
        filter,
        file_pruning_predicate: None,
        expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
        table_schema,
        limit: None,
        metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
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

#[rstest]
#[tokio::test]
async fn test_residual_filter_unprojected_column(
    #[values(false, true)] projection_pushdown: bool,
    #[values(false, true)] reordered_schema: bool,
) -> anyhow::Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = record_batch!(
        ("a", Int32, vec![10, 20, 30, 40]),
        ("unused", Int32, vec![0, 0, 0, 0]),
        ("b", Int32, vec![Some(1), Some(2), None, Some(4)])
    )?;
    let size = write_arrow_to_vortex(Arc::clone(&store), "residual.vortex", batch.clone()).await?;
    let schema = Arc::new(batch.schema().project(if reordered_schema {
        &[2, 0, 1]
    } else {
        &[0, 1, 2]
    })?);
    let modulo: PhysicalExprRef = Arc::new(df_expr::BinaryExpr::new(
        Arc::new(df_expr::Column::new("b", schema.index_of("b")?)),
        Operator::Modulo,
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(2)))),
    ));
    let filter = Arc::new(df_expr::BinaryExpr::new(
        modulo,
        Operator::Eq,
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(0)))),
    ));
    let mut opener = make_opener(store, TableSchema::from(Arc::clone(&schema)), Some(filter));
    opener.projection = ProjectionExprs::from_indices(&[schema.index_of("a")?], &schema);
    opener.projection_pushdown = projection_pushdown;
    opener.limit = Some(1);
    let batches = opener
        .open(PartitionedFile::new("residual.vortex", size))?
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_batches_eq!(
        ["+----+", "| a  |", "+----+", "| 20 |", "| 40 |", "+----+"],
        &batches
    );
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
async fn test_open_all_valid_nullable_columns_with_nonnullable_table_schema() -> anyhow::Result<()>
{
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
async fn test_file_pruning_replaces_partition_columns_without_file_statistics() -> anyhow::Result<()>
{
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
        vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(Arc::clone(&object_store))),
        projection: ProjectionExprs::from_indices(&[0], table_schema.file_schema()),
        filter: Some(filter),
        file_pruning_predicate: None,
        expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
        table_schema: table_schema.clone(),
        limit: None,
        metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
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
        projection: ProjectionExprs::from_indices(&[0, 1, 2], &table_schema),
        filter: None,
        file_pruning_predicate: None,
        expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
        table_schema: TableSchema::from(Arc::clone(&table_schema)),
        limit: None,
        metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
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
        vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(Arc::clone(&object_store))),
        projection: ProjectionExprs::from_indices(projection.as_ref(), table_schema.file_schema()),
        filter: None,
        file_pruning_predicate: None,
        expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
        table_schema: table_schema.clone(),
        limit: None,
        metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
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
        projection,
        filter: None,
        file_pruning_predicate: None,
        expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
        table_schema: TableSchema::from(schema),
        limit: None,
        metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
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
    file.extensions.insert(
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
    file.extensions.insert(
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
        vortex_reader_factory: Arc::new(DefaultVortexReaderFactory::new(Arc::clone(&object_store))),
        projection,
        filter: None,
        file_pruning_predicate: None,
        expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
        table_schema,
        limit: None,
        metrics_registry: Arc::new(DefaultMetricsRegistry::default()),
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

#[derive(Debug)]
struct ModuloAdapterFactory;

#[derive(Debug)]
struct ModuloAdapter(Arc<dyn datafusion_physical_expr_adapter::PhysicalExprAdapter>);

impl PhysicalExprAdapterFactory for ModuloAdapterFactory {
    fn create(
        &self,
        logical: SchemaRef,
        physical: SchemaRef,
    ) -> DFResult<Arc<dyn datafusion_physical_expr_adapter::PhysicalExprAdapter>> {
        Ok(Arc::new(ModuloAdapter(
            DefaultPhysicalExprAdapterFactory.create(logical, physical)?,
        )))
    }
}

impl datafusion_physical_expr_adapter::PhysicalExprAdapter for ModuloAdapter {
    fn rewrite(&self, expr: PhysicalExprRef) -> DFResult<PhysicalExprRef> {
        self.0
            .rewrite(expr)?
            .transform_up(|expr| {
                if expr
                    .downcast_ref::<df_expr::Column>()
                    .is_some_and(|c| c.name() == "b")
                {
                    Ok(Transformed::yes(Arc::new(df_expr::BinaryExpr::new(
                        expr,
                        Operator::Modulo,
                        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(2)))),
                    )) as PhysicalExprRef))
                } else {
                    Ok(Transformed::no(expr))
                }
            })
            .map(|result| result.data)
    }
}

struct DelegatingConvertor(DefaultExpressionConvertor);

impl ExpressionConvertor for DelegatingConvertor {
    fn try_convert(
        &self,
        expr: &PhysicalExprRef,
        schema: &Schema,
    ) -> DFResult<Option<vortex::expr::Expression>> {
        self.0.try_convert(expr, schema)
    }
}

#[rstest]
#[tokio::test]
async fn test_adapted_filter_fallback_and_limit(
    #[values(false, true)] projection_pushdown: bool,
    #[values(false, true)] custom_convertor: bool,
    #[values(false, true)] zero_columns: bool,
) -> anyhow::Result<()> {
    let ctx = crate::common_tests::TestSessionContext::new(projection_pushdown);
    let batch = record_batch!(
        ("a", Int32, vec![10, 20, 30, 40]),
        ("b", Int32, vec![Some(1), None, Some(2), Some(4)])
    )?;
    ctx.write_arrow_batch("adapted.vortex", &batch).await?;
    let metadata = ctx.store.head(&Path::from("adapted.vortex")).await?;
    let mut source = crate::VortexSource::new(TableSchema::from(batch.schema()), SESSION.clone())
        .with_projection_pushdown(projection_pushdown);
    if custom_convertor {
        source = source.with_expression_convertor(Arc::new(DelegatingConvertor(
            DefaultExpressionConvertor::default(),
        )));
    }
    let filter: PhysicalExprRef = Arc::new(df_expr::BinaryExpr::new(
        Arc::new(df_expr::Column::new("b", 1)),
        Operator::Eq,
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(0)))),
    ));
    let accepted = datafusion_datasource::file::FileSource::try_pushdown_filters(
        &source,
        vec![filter],
        &datafusion_common::config::ConfigOptions::new(),
    )?;
    assert!(matches!(
        accepted.filters.as_slice(),
        [datafusion_physical_plan::filter_pushdown::PushedDown::Yes]
    ));
    let mut source = accepted
        .updated_node
        .ok_or_else(|| anyhow::anyhow!("Expected updated source"))?;
    // Alias a computed output to b, which is also an unprojected residual input.
    let projection = if zero_columns {
        ProjectionExprs::from(Vec::<ProjectionExpr>::new())
    } else {
        vec![ProjectionExpr {
            expr: Arc::new(df_expr::BinaryExpr::new(
                Arc::new(df_expr::Column::new("a", 0)),
                Operator::Plus,
                Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(1)))),
            )),
            alias: "b".into(),
        }]
        .into()
    };
    source = source
        .try_pushdown_projection(&projection)?
        .ok_or_else(|| anyhow::anyhow!("Expected projected source"))?;
    let config = datafusion_datasource::file_scan_config::FileScanConfigBuilder::new(
        datafusion_execution::object_store::ObjectStoreUrl::local_filesystem(),
        source,
    )
    .with_expr_adapter(Some(Arc::new(ModuloAdapterFactory)))
    .with_limit(Some(1))
    .with_file(PartitionedFile::new("adapted.vortex", metadata.size))
    .build();
    let plan: Arc<dyn datafusion_physical_plan::ExecutionPlan> = Arc::new(
        datafusion_datasource::source::DataSourceExec::new(Arc::new(config)),
    );
    let batches = datafusion_physical_plan::collect(plan, ctx.session.task_ctx()).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    if zero_columns {
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    } else {
        assert_batches_eq!(["+----+", "| b  |", "+----+", "| 31 |", "+----+"], &batches);
    }
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_temporal_adapter_residual(
    #[values(false, true)] projection_pushdown: bool,
) -> anyhow::Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(arrow_array::TimestampMillisecondArray::from(vec![
                Some(1_000),
                None,
                Some(2_000),
            ])),
        ],
    )?;
    let size = write_arrow_to_vortex(Arc::clone(&store), "temporal.vortex", batch).await?;
    let logical = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let filter: PhysicalExprRef = Arc::new(df_expr::BinaryExpr::new(
        Arc::new(df_expr::Column::new("ts", 1)),
        Operator::Eq,
        Arc::new(df_expr::Literal::new(ScalarValue::TimestampMicrosecond(
            Some(1_000_000),
            None,
        ))),
    ));
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&filter, &logical)?
            .is_some()
    );
    let mut opener = make_opener(store, TableSchema::from(logical), Some(filter));
    opener.projection_pushdown = projection_pushdown;
    let batches = opener
        .open(PartitionedFile::new("temporal.vortex", size))?
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_batches_eq!(["+----+", "| a  |", "+----+", "| 10 |", "+----+"], &batches);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_residual_literal_retains_zero_column_rows(
    #[values(Some(true), Some(false), None)] value: Option<bool>,
) -> anyhow::Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = record_batch!(("a", Int32, vec![1, 2, 3]))?;
    let size = write_arrow_to_vortex(Arc::clone(&store), "literal.vortex", batch.clone()).await?;
    let mut opener = make_opener(
        store,
        TableSchema::from(batch.schema()),
        Some(Arc::new(df_expr::Literal::new(ScalarValue::Boolean(value)))),
    );
    opener.projection = Vec::<ProjectionExpr>::new().into();
    // Force a supported physical expression to use the residual path.
    opener.expression_convertor = Arc::new(ResidualConvertor);
    let batches = opener
        .open(PartitionedFile::new("literal.vortex", size))?
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        if value == Some(true) { 3 } else { 0 }
    );
    Ok(())
}

struct ResidualConvertor;
impl ExpressionConvertor for ResidualConvertor {
    fn try_convert(
        &self,
        _expr: &PhysicalExprRef,
        _schema: &Schema,
    ) -> DFResult<Option<vortex::expr::Expression>> {
        Ok(None)
    }
}

#[rstest]
#[tokio::test]
async fn test_physical_in_list(
    #[values(false, true)] negated: bool,
    #[values(0, 1, 2)] list_kind: usize,
) -> anyhow::Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = record_batch!(
        ("id", Int32, vec![0, 1, 2]),
        ("a", Int32, vec![Some(1), Some(2), None]),
        ("b", Int32, vec![Some(1), None, Some(2)])
    )?;
    let size = write_arrow_to_vortex(Arc::clone(&store), "in-list.vortex", batch.clone()).await?;
    let list: Vec<PhysicalExprRef> = match list_kind {
        0 => vec![],
        1 => vec![Arc::new(df_expr::Column::new("b", 2))],
        _ => vec![
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(1)))),
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(None))),
        ],
    };
    let filter: PhysicalExprRef = Arc::new(df_expr::InListExpr::try_new(
        Arc::new(df_expr::Column::new("a", 1)),
        list,
        negated,
        &batch.schema(),
    )?);
    assert_eq!(
        DefaultExpressionConvertor::default()
            .try_convert(&filter, &batch.schema())?
            .is_some(),
        list_kind == 2,
    );
    let expected = batch_filter(&batch, &filter)?.project(&[0])?;
    let opener = make_opener(store, TableSchema::from(batch.schema()), Some(filter));
    let actual = opener
        .open(PartitionedFile::new("in-list.vortex", size))?
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let actual = concat_batches(&expected.schema(), &actual)?;
    assert_eq!(actual, expected);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_native_and_residual_composition(
    #[values(Operator::And, Operator::Or)] operator: Operator,
) -> anyhow::Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = record_batch!(
        ("a", Int32, vec![10, 20, 30, 40]),
        ("b", Int32, vec![Some(1), None, Some(2), Some(4)])
    )?;
    let size =
        write_arrow_to_vortex(Arc::clone(&store), "composition.vortex", batch.clone()).await?;
    let native: PhysicalExprRef = Arc::new(df_expr::BinaryExpr::new(
        Arc::new(df_expr::Column::new("a", 0)),
        Operator::Gt,
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(35)))),
    ));
    let residual: PhysicalExprRef = Arc::new(df_expr::BinaryExpr::new(
        Arc::new(df_expr::BinaryExpr::new(
            Arc::new(df_expr::Column::new("b", 1)),
            Operator::Modulo,
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(2)))),
        )),
        Operator::Eq,
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(0)))),
    ));
    let filter: PhysicalExprRef = Arc::new(df_expr::BinaryExpr::new(native, operator, residual));
    let expected = batch_filter(&batch, &filter)?.project(&[0])?;
    let opener = make_opener(store, TableSchema::from(batch.schema()), Some(filter));
    let actual = opener
        .open(PartitionedFile::new("composition.vortex", size))?
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let actual = concat_batches(&expected.schema(), &actual)?;
    assert_eq!(actual, expected);
    Ok(())
}

#[tokio::test]
async fn test_residual_error_has_file_and_predicate() -> anyhow::Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = record_batch!(("a", Int32, vec![1, 2]))?;
    let size =
        write_arrow_to_vortex(Arc::clone(&store), "filter-error.vortex", batch.clone()).await?;
    let filter: PhysicalExprRef = Arc::new(df_expr::BinaryExpr::new(
        Arc::new(df_expr::BinaryExpr::new(
            Arc::new(df_expr::Column::new("a", 0)),
            Operator::Divide,
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(0)))),
        )),
        Operator::Eq,
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(1)))),
    ));
    let mut opener = make_opener(store, TableSchema::from(batch.schema()), Some(filter));
    opener.expression_convertor = Arc::new(ResidualConvertor);
    let result = opener
        .open(PartitionedFile::new("filter-error.vortex", size))?
        .await?
        .try_collect::<Vec<_>>()
        .await;
    let error = result
        .err()
        .ok_or_else(|| anyhow::anyhow!("Expected division by zero"))?
        .to_string();
    assert!(error.contains("filter-error.vortex"), "{error}");
    assert!(error.contains("residual filter"), "{error}");
    Ok(())
}

#[tokio::test]
async fn test_residual_rejects_rows_before_projection_evaluation() -> anyhow::Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let batch = record_batch!(("a", Int32, vec![1, 2]))?;
    let size =
        write_arrow_to_vortex(Arc::clone(&store), "empty-projection.vortex", batch.clone()).await?;
    let mut opener = make_opener(
        store,
        TableSchema::from(batch.schema()),
        Some(Arc::new(df_expr::Literal::new(ScalarValue::Boolean(Some(
            false,
        ))))),
    );
    opener.expression_convertor = Arc::new(ResidualConvertor);
    opener.projection = vec![ProjectionExpr {
        expr: Arc::new(SnapshotErrorExpr),
        alias: "failure".into(),
    }]
    .into();
    let actual = opener
        .open(PartitionedFile::new("empty-projection.vortex", size))?
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    Ok(())
}
