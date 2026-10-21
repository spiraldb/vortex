// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use futures::FutureExt;
use futures::TryStreamExt;
use futures::stream;
use parking_lot::Condvar;
use parking_lot::Mutex;
use rstest::rstest;
use vortex_array::ArrayContext;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::VarBinArray;
use vortex_array::assert_arrays_eq;
use vortex_array::dtype::DType;
use vortex_array::expr::get_item;
use vortex_array::expr::gt;
use vortex_array::expr::lit;
use vortex_array::expr::pack;
use vortex_array::expr::root;
use vortex_array::validity::Validity;
use vortex_arrow::ArrowSessionExt;
use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_io::session::RuntimeSessionExt;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_session::VortexSession;

use super::MAX_ROWS_PER_CHUNK;
use super::PreparedScan;
use super::ScanBuilder;
use super::ScanFile;
use super::rows_per_chunk;
use crate::LayoutRef;
use crate::LayoutStrategy;
use crate::layouts::chunked::writer::ChunkedLayoutStrategy;
use crate::layouts::dict::DictLayout;
use crate::layouts::flat::writer::FlatLayoutStrategy;
use crate::layouts::row_idx::row_idx;
use crate::layouts::table::TableStrategy;
use crate::layouts::zoned::writer::ZonedLayoutOptions;
use crate::layouts::zoned::writer::ZonedStrategy;
use crate::scan::execution::FilePlanner;
use crate::scan::execution::SegmentLocation;
use crate::scan::scan_builder::ScanBuilder as V1ScanBuilder;
use crate::scan::v2::split::SplitPlan;
use crate::segments::SegmentFuture;
use crate::segments::SegmentId;
use crate::segments::SegmentSource;
use crate::segments::TestSegments;
use crate::sequence::SequenceId;
use crate::sequence::SequentialStreamAdapter;
use crate::sequence::SequentialStreamExt as _;
use crate::test::new_session;

const CHUNK_ROWS: usize = 1000;
const CHUNKS: usize = 4;

struct RangeCountingSource {
    inner: Arc<dyn SegmentSource>,
    bytes: Arc<AtomicUsize>,
}

impl SegmentSource for RangeCountingSource {
    fn supports_ranges(&self) -> bool {
        true
    }

    fn request(&self, id: SegmentId) -> SegmentFuture {
        let read = self.inner.request(id);
        let bytes = Arc::clone(&self.bytes);
        async move {
            let buffer = read.await?;
            bytes.fetch_add(buffer.len(), Ordering::Relaxed);
            Ok(buffer)
        }.boxed()
    }

    fn request_range(&self, id: SegmentId, range: Range<usize>) -> SegmentFuture {
        self.bytes.fetch_add(range.len(), Ordering::Relaxed);
        self.inner.request_range(id, range)
    }
}

#[rstest]
#[case(false, vec![0, 1, 65535, 65536, 131071, 199999], true)]
#[case(true, vec![0, 65535, 199999], false)]
#[case(false, (0..50000).collect(), false)]
#[tokio::test]
async fn sparse_primitive_reads_preserve_rows_and_skip_bytes(
    #[case] nullable: bool,
    #[case] indices: Vec<u64>,
    #[case] sparse: bool,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let values: Vec<i64> = (0..200000_i64).map(|value| value.wrapping_mul(7919) - 12345).collect();
    let expected = PrimitiveArray::new(
        Buffer::from_iter(indices.iter().map(|&index| values[index as usize])),
        if nullable { Validity::AllValid } else { Validity::NonNullable },
    );
    let array = PrimitiveArray::new(
        Buffer::from(values),
        if nullable { Validity::AllValid } else { Validity::NonNullable },
    ).into_array();
    let segments = Arc::new(TestSegments::default());
    let (sequence, eof) = SequenceId::root().split();
    let layout = FlatLayoutStrategy::default().write_stream(
        ArrayContext::empty().into(),
        Arc::clone(&segments) as _,
        SequentialStreamAdapter::new(array.dtype().clone(), stream::iter([Ok((sequence, array))])).sendable(),
        eof,
        &session,
    ).await?;
    let id = layout.segment_ids()[0];
    let length = segments.request(id).await?.len();
    let bytes = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(RangeCountingSource { inner: segments, bytes: Arc::clone(&bytes) });
    let file = ScanFile {
        layout,
        locations: Arc::from([SegmentLocation { offset: 8, length: u32::try_from(length)?, alignment: Alignment::new(8) }]),
        segments: source,
    };
    let dtype = file.layout.dtype().clone();
    let actual = collect(dtype, ScanBuilder::new(session.clone(), file)
        .with_row_indices(StrictSortedBuffer::try_new(Buffer::from(indices))?)
        .into_stream()?).await?;
    assert_arrays_eq!(actual, expected, &mut session.create_execution_ctx());
    if sparse {
        assert!(bytes.load(Ordering::Relaxed) < length / 8);
    } else {
        assert!(bytes.load(Ordering::Relaxed) >= length);
    }
    Ok(())
}

/// A table chunk of `CHUNK_ROWS` rows starting at row `start`: an integer, a nullable string and a
/// list column.
fn table_chunk(start: usize) -> VortexResult<ArrayRef> {
    let rows = start..start + CHUNK_ROWS;
    let ints = Buffer::from_iter(
        rows.clone()
            .map(|row| i32::try_from(row).unwrap_or(i32::MAX)),
    );
    let strings = VarBinArray::from_iter(
        rows.map(|row| (row % 3 != 0).then(|| format!("row {row}"))),
        DType::Utf8(true.into()),
    );
    let elements = Buffer::from_iter((0..2 * CHUNK_ROWS).map(|i| i64::try_from(i).unwrap_or(0)));
    let offsets = Buffer::from_iter((0..=CHUNK_ROWS).map(|i| u32::try_from(2 * i).unwrap_or(0)));
    let lists = ListArray::try_new(
        elements.into_array(),
        offsets.into_array(),
        Validity::NonNullable,
    )?;
    Ok(StructArray::from_fields(&[
        ("ints", ints.into_array()),
        ("strings", strings.into_array()),
        ("lists", lists.into_array()),
    ])?
    .into_array())
}

/// Writes `CHUNKS` table chunks with `strategy`.
async fn write(
    session: &VortexSession,
    strategy: &dyn LayoutStrategy,
) -> VortexResult<(Arc<dyn SegmentSource>, LayoutRef)> {
    let segments = Arc::new(TestSegments::default());
    let (mut sequence_id, eof) = SequenceId::root().split();
    let chunks = (0..CHUNKS)
        .map(|chunk| Ok((sequence_id.advance(), table_chunk(chunk * CHUNK_ROWS)?)))
        .collect::<Vec<_>>();
    let dtype = table_chunk(0)?.dtype().clone();
    let layout = strategy
        .write_stream(
            ArrayContext::empty().into(),
            Arc::<TestSegments>::clone(&segments),
            SequentialStreamAdapter::new(dtype, stream::iter(chunks)).sendable(),
            eof,
            session,
        )
        .await?;
    Ok((segments, layout))
}

fn chunked_flat() -> Arc<dyn LayoutStrategy> {
    Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()))
}

/// Every column is shredded into its own chunked leaves.
fn table() -> Arc<dyn LayoutStrategy> {
    Arc::new(TableStrategy::new(chunked_flat(), chunked_flat()))
}

/// Every column is shredded into chunked leaves with zone statistics.
fn zoned_table() -> Arc<dyn LayoutStrategy> {
    let zoned = ZonedStrategy::new(
        ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()),
        FlatLayoutStrategy::default(),
        ZonedLayoutOptions {
            block_size: NonZeroUsize::new(CHUNK_ROWS).unwrap_or(NonZeroUsize::MIN),
            ..Default::default()
        },
    );
    Arc::new(TableStrategy::new(chunked_flat(), Arc::new(zoned)))
}

/// A [`ScanFile`] over test segments. They have no byte offsets, and the scan fetches them by id,
/// so each location is a placeholder whose offset is its id.
fn scan_file(segments: &Arc<dyn SegmentSource>, layout: &LayoutRef) -> VortexResult<ScanFile> {
    let mut count = 0;
    for layout in layout.depth_first_traversal() {
        for id in layout?.segment_ids() {
            count = count.max(*id as usize + 1);
        }
    }
    let locations = (0..count as u64)
        .map(|offset| SegmentLocation {
            offset,
            length: 0,
            alignment: Alignment::none(),
        })
        .collect();
    Ok(ScanFile {
        layout: Arc::clone(layout),
        locations,
        segments: Arc::clone(segments),
    })
}

fn builder(
    session: &VortexSession,
    segments: &Arc<dyn SegmentSource>,
    layout: &LayoutRef,
) -> VortexResult<ScanBuilder<ArrayRef>> {
    Ok(ScanBuilder::new(
        session.clone(),
        scan_file(segments, layout)?,
    ))
}

async fn collect(
    dtype: DType,
    stream: impl futures::Stream<Item = VortexResult<ArrayRef>>,
) -> VortexResult<ArrayRef> {
    let chunks: Vec<ArrayRef> = stream.try_collect().await?;
    Ok(ChunkedArray::try_new(chunks, dtype)?.into_array())
}

/// Runs `builder` on both executors and checks the planner-driven one returns the same rows.
async fn assert_same_as_v1(
    session: &VortexSession,
    make: impl Fn() -> VortexResult<ScanBuilder<ArrayRef>>,
) -> VortexResult<()> {
    let builder = make()?;
    let dtype = builder.dtype().clone();
    let reader = builder.file.layout.new_reader(
        "".into(),
        Arc::clone(&builder.file.segments),
        session,
        &Default::default(),
    )?;
    let mut v1 = V1ScanBuilder::new(session.clone(), reader)
        .with_projection(builder.projection.clone())
        .with_selection(builder.selection.clone())
        .with_row_offset(builder.row_offset)
        .with_ordered(builder.ordered)
        .with_concurrency(builder.concurrency);
    if let Some(rows) = builder.row_range.clone() {
        v1 = v1.with_row_range(rows);
    }
    let expected = collect(dtype.clone(), v1.into_stream()?).await?;
    let actual = collect(dtype, builder.into_stream()?).await?;
    assert_arrays_eq!(actual, expected, &mut session.create_execution_ctx());
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Strategy {
    Flat,
    Table,
    ZonedTable,
}

impl Strategy {
    fn get(self) -> Arc<dyn LayoutStrategy> {
        match self {
            Strategy::Flat => chunked_flat(),
            Strategy::Table => table(),
            Strategy::ZonedTable => zoned_table(),
        }
    }
}

#[rstest]
#[case::whole(None, None)]
#[case::row_range(Some(250..3_100), None)]
#[case::empty_row_range(Some(10..10), None)]
#[case::empty_indices(None, Some(&[][..]))]
#[case::row_indices(None, Some(&[0, 1, 999, 1000, 2500, 3999][..]))]
#[case::row_range_and_indices(Some(900..2600), Some(&[0, 999, 1000, 2500, 3999][..]))]
#[case::indices_outside_range(Some(900..2600), Some(&[0, 3999][..]))]
#[tokio::test]
async fn matches_v1(
    #[values(Strategy::Flat, Strategy::Table, Strategy::ZonedTable)] strategy: Strategy,
    #[case] row_range: Option<Range<u64>>,
    #[case] rows: Option<&'static [u64]>,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, strategy.get().as_ref()).await?;
    assert_same_as_v1(&session, || {
        let mut builder = builder(&session, &segments, &layout)?;
        if let Some(row_range) = row_range.clone() {
            builder = builder.with_row_range(row_range);
        }
        if let Some(rows) = rows {
            builder =
                builder.with_row_indices(StrictSortedBuffer::try_new(Buffer::copy_from(rows))?);
        }
        Ok(builder)
    })
    .await
}

#[rstest]
#[tokio::test]
async fn projection_matches_v1(
    #[values(Strategy::Flat, Strategy::Table, Strategy::ZonedTable)] strategy: Strategy,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, strategy.get().as_ref()).await?;
    let dtype = layout.dtype().clone();
    assert_same_as_v1(&session, || {
        let projection = pack(
            [
                ("strings", get_item("strings", root())),
                ("idx", row_idx()),
                ("big", gt(get_item("ints", root()), lit(1500i32))),
            ],
            false.into(),
        );
        Ok(builder(&session, &segments, &layout)?
            .with_projection(projection.bind(&dtype)?)
            .with_row_offset(100))
    })
    .await
}

#[tokio::test]
async fn arrow_stream_projects_across_chunks() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let projection =
        pack([("value", get_item("ints", root()))], false.into()).bind(layout.dtype())?;
    let scan = builder(&session, &segments, &layout)?
        .with_projection(projection)
        .with_row_range(990..1010);
    let schema = Arc::new(session.arrow().to_arrow_schema(scan.dtype())?);
    let batches: Vec<_> = scan.into_record_batch_stream(schema)?.try_collect().await?;
    let values: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, (990_i32..1010).collect::<Vec<_>>());
    Ok(())
}

#[rstest]
#[case::empty(0, 0, 65_536)]
#[case::single_chunk(1000, 100_000_000, 65_536)]
#[case::no_bytes(100_000, 0, 65_536)]
#[case::small_rows(1_000_000, 64_000_000, 65_536)]
#[case::large_rows(1_000_000, 384_000_000, 16_384)]
#[case::minimum(1_000_000, 2_000_000_000, 8192)]
fn chunk_byte_budget(#[case] rows: u64, #[case] bytes: u32, #[case] expected: usize) {
    let locations = [SegmentLocation {
        offset: 0,
        length: bytes,
        alignment: Alignment::none(),
    }];
    assert_eq!(rows_per_chunk(rows, &locations), expected);
}

struct CountingSegments {
    inner: Arc<dyn SegmentSource>,
    reads: AtomicUsize,
}

struct ConcurrentSegments {
    inner: Arc<dyn SegmentSource>,
    started: Mutex<usize>,
    ready: Condvar,
}

impl SegmentSource for ConcurrentSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let mut started = self.started.lock();
        *started += 1;
        self.ready.notify_all();
        let timeout =
            self.ready
                .wait_while_for(&mut started, |started| *started < 2, Duration::from_secs(5));
        drop(started);
        assert!(
            !timeout.timed_out(),
            "a second child must run while the first child is blocked"
        );
        self.inner.request(id)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_planner_schedules_children_on_independent_workers() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let segments = Arc::new(ConcurrentSegments {
        inner: segments,
        started: Mutex::new(0),
        ready: Condvar::new(),
    }) as Arc<dyn SegmentSource>;
    let mut scan = PreparedScan::try_new(
        builder(&session, &segments, &layout)?
            .with_projection(get_item("ints", root()).bind(layout.dtype())?)
            .with_row_indices(StrictSortedBuffer::try_new(Buffer::from(vec![7, 3007]))?),
    )?;
    scan.chunk_rows = CHUNK_ROWS;
    let actual = collect(
        scan.plans.projection.dtype().clone(),
        scan.execute_stream()?,
    )
    .await?;
    assert_arrays_eq!(
        actual,
        Buffer::from(vec![7_i32, 3007]).into_array(),
        &mut session.create_execution_ctx()
    );
    Ok(())
}

impl SegmentSource for CountingSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.request(id)
    }
}

#[tokio::test]
async fn dictionary_values_are_reused_between_splits() -> VortexResult<()> {
    let session = new_session().with_tokio();
    const STORED_CHUNK_ROWS: usize = 70_000;
    const STORED_CHUNKS: usize = 3;
    let segments = Arc::new(TestSegments::default());
    let (mut sequence, eof) = SequenceId::root().split();
    let arrays = (0..STORED_CHUNKS)
        .map(|index| {
            let start = i32::try_from(index * STORED_CHUNK_ROWS)?;
            let end = start + i32::try_from(STORED_CHUNK_ROWS)?;
            Ok((
                sequence.advance(),
                PrimitiveArray::from_iter(start..end).into_array(),
            ))
        })
        .collect::<VortexResult<Vec<_>>>()?;
    let dtype = DType::Primitive(vortex_array::dtype::PType::I32, false.into());
    let ints = chunked_flat()
        .write_stream(
            ArrayContext::empty().into(),
            Arc::<TestSegments>::clone(&segments),
            SequentialStreamAdapter::new(dtype, stream::iter(arrays.into_iter().map(Ok)))
                .sendable(),
            eof,
            &session,
        )
        .await?;
    // Codes index the same values across fixed work chunks and stored chunk boundaries.
    let layout = DictLayout::new(Arc::clone(&ints), ints).into_layout();
    let counted = Arc::new(CountingSegments {
        inner: segments,
        reads: AtomicUsize::new(0),
    });
    let segments = Arc::clone(&counted) as Arc<dyn SegmentSource>;
    let scan = PreparedScan::try_new(builder(&session, &segments, &layout)?)?;
    let mut planner = FilePlanner::new(
        scan.plans.clone(),
        scan.rows.clone(),
        scan.chunk_rows,
        scan.selection.clone(),
    );
    let splits = (STORED_CHUNKS * STORED_CHUNK_ROWS).div_ceil(MAX_ROWS_PER_CHUNK);
    for index in 0..splits {
        let PlannerOutput::Planner(scope, root) = planner.compute()? else {
            vortex_bail!("all rows must have a projection planner");
        };
        let split = SplitPlan { scope, root };
        let rows = split.scope.rows.clone();
        let before = counted.reads.load(Ordering::Relaxed);
        let arrays = split.run(Arc::clone(&scan.io)).await?;
        let reads = counted.reads.load(Ordering::Relaxed) - before;
        let code_chunks =
            (rows.end - 1) / STORED_CHUNK_ROWS as u64 - rows.start / STORED_CHUNK_ROWS as u64 + 1;
        assert_eq!(
            reads,
            if index == 0 {
                STORED_CHUNKS
            } else {
                usize::try_from(code_chunks)?
            }
        );
        let expected =
            PrimitiveArray::from_iter(i32::try_from(rows.start)?..i32::try_from(rows.end)?);
        assert_arrays_eq!(
            ChunkedArray::try_new(arrays, layout.dtype().clone())?.into_array(),
            expected,
            &mut session.create_execution_ctx()
        );
    }
    assert_eq!(planner.state(), State::Done);
    Ok(())
}

/// V2 rejects list layouts: their element ranges are only known once their offsets are read.
#[tokio::test]
async fn list_layouts_return_an_error() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let strategy = TableStrategy::new(chunked_flat(), chunked_flat()).with_list_layout();
    let (segments, layout) = write(&session, &strategy).await?;
    let builder = builder(&session, &segments, &layout)?;
    let error = builder
        .into_stream()?
        .try_next()
        .await
        .expect_err("V2 must reject an unsupported layout");
    assert!(error.to_string().contains("vortex.plan.list_pack"));
    Ok(())
}

#[tokio::test]
async fn unordered_returns_every_row() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let scan = PreparedScan::try_new(builder(&session, &segments, &layout)?.with_ordered(false))?;
    let chunks: Vec<ArrayRef> = scan.execute_stream()?.try_collect().await?;
    let rows: usize = chunks.iter().map(|chunk| chunk.len()).sum();
    assert_eq!(rows, CHUNKS * CHUNK_ROWS);
    Ok(())
}

#[tokio::test]
async fn primitive_column() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let dtype = layout.dtype().clone();
    let scan = PreparedScan::try_new(
        builder(&session, &segments, &layout)?
            .with_projection(get_item("ints", root()).bind(&dtype)?)
            .with_row_range(990..1010),
    )?;
    let actual = collect(
        DType::Primitive(vortex_array::dtype::PType::I32, false.into()),
        scan.execute_stream()?,
    )
    .await?;
    assert_arrays_eq!(
        actual,
        PrimitiveArray::from_iter(990i32..1010),
        &mut session.create_execution_ctx()
    );
    Ok(())
}

/// Stored chunk boundaries do not subdivide a fixed work chunk.
#[rstest]
#[tokio::test]
async fn work_chunks_ignore_layout_boundaries(
    #[values(Strategy::Flat, Strategy::Table, Strategy::ZonedTable)] strategy: Strategy,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, strategy.get().as_ref()).await?;
    let scan = PreparedScan::try_new(builder(&session, &segments, &layout)?)?;
    assert_eq!(scan.rows, 0..(CHUNKS * CHUNK_ROWS) as u64);
    let arrays: Vec<_> = scan.execute_stream()?.try_collect().await?;
    assert_eq!(arrays.len(), 1);
    assert_eq!(arrays[0].len(), CHUNKS * CHUNK_ROWS);
    Ok(())
}

#[tokio::test]
async fn selected_split_scope_is_bounded_before_projection() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let scan = PreparedScan::try_new(
        builder(&session, &segments, &layout)?
            .with_row_indices(StrictSortedBuffer::try_new(Buffer::from(vec![7, 2001]))?),
    )?;
    let mut planner = FilePlanner::new(scan.plans, scan.rows, scan.chunk_rows, scan.selection);
    let PlannerOutput::Planner(scope, _) = planner.compute()? else {
        vortex_bail!("selected chunk must have a projection planner");
    };
    assert_eq!(scope.rows, 7..2002);
    assert_eq!(planner.state(), State::Done);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn dropping_file_planner_stream_releases_pending_work(
    #[values(false, true)] consume_first: bool,
    #[values(1, 4)] lookahead: usize,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let scan = PreparedScan::try_new(builder(&session, &segments, &layout)?)?;
    let locations = Arc::downgrade(&scan.plans.locations);
    let mut children =
        FilePlanner::new(scan.plans, scan.rows, 1, scan.selection).into_stream(lookahead);
    if consume_first {
        assert!(children.try_next().await?.is_some());
    }
    drop(children);
    tokio::task::yield_now().await;
    assert!(
        locations.upgrade().is_none(),
        "dropping the stream must cancel the producer and release queued planners"
    );
    Ok(())
}
