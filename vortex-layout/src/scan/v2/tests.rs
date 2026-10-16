// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use futures::TryStreamExt;
use futures::stream;
use rstest::rstest;
use vortex_array::ArrayContext;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::VarBinArray;
use vortex_array::assert_arrays_eq;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldNames;
use vortex_array::expr::get_item;
use vortex_array::expr::gt;
use vortex_array::expr::lit;
use vortex_array::expr::pack;
use vortex_array::expr::root;
use vortex_array::flatbuffers::WriteFlatBufferExt;
use vortex_array::validity::Validity;
use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_io::session::RuntimeSessionExt;
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;

use super::Executor;
use super::ScanFile;
use super::select_supported;
use crate::LayoutChildren;
use crate::LayoutContext;
use crate::LayoutEncodingRef;
use crate::LayoutId;
use crate::LayoutRef;
use crate::LayoutStrategy;
use crate::layout_from_flatbuffer_with_options;
use crate::layouts::chunked::Chunked;
use crate::layouts::chunked::ChunkedLayout;
use crate::layouts::chunked::writer::ChunkedLayoutStrategy;
use crate::layouts::dict::DictLayout;
use crate::layouts::flat::writer::FlatLayoutStrategy;
use crate::layouts::foreign::new_foreign_layout;
use crate::layouts::list::ListLayout;
use crate::layouts::list::writer::ListLayoutStrategy;
use crate::layouts::row_idx::row_idx;
use crate::layouts::struct_::StructLayout;
use crate::layouts::table::TableStrategy;
use crate::layouts::zoned::writer::ZonedLayoutOptions;
use crate::layouts::zoned::writer::ZonedStrategy;
use crate::plan::physical::PlanRef;
use crate::scan::planning::SegmentLocation;
use crate::scan::scan_builder::ScanBuilder;
use crate::scan::split_by::SplitBy;
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
    Ok(ScanFile::new(
        Arc::clone(layout),
        locations,
        Arc::clone(segments),
    ))
}

fn builder(
    session: &VortexSession,
    segments: &Arc<dyn SegmentSource>,
    layout: &LayoutRef,
) -> VortexResult<ScanBuilder<ArrayRef>> {
    let reader = layout.new_reader(
        "".into(),
        Arc::clone(segments),
        session,
        &Default::default(),
    )?;
    Ok(ScanBuilder::new(session.clone(), reader).with_scan_file(scan_file(segments, layout)?))
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
    let dtype = make()?.dtype()?;
    let expected = collect(dtype.clone(), make()?.prepare()?.execute_stream(None)?).await?;
    let Executor::V2(scan) = select_supported(make()?)? else {
        panic!("the scan should run on the planner-driven executor");
    };
    let actual = collect(dtype, scan.execute_stream()?).await?;
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
#[case::row_indices(None, Some(&[0, 1, 999, 1000, 2500, 3999][..]))]
#[case::row_range_and_indices(Some(900..2600), Some(&[0, 999, 1000, 2500, 3999][..]))]
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
#[case::empty(&[])]
#[case::first(&[0])]
#[case::last(&[3999])]
#[case::boundaries(&[0, 999, 1000, 1999, 2000, 3999])]
#[case::scattered(&[1, 17, 1002, 2301, 3333])]
#[tokio::test]
async fn selection_matches_full_scan_take(
    #[values(Strategy::Flat, Strategy::Table, Strategy::ZonedTable)] strategy: Strategy,
    #[values(false, true)] roaring: bool,
    #[values(false, true)] exclude: bool,
    #[case] rows: &[u64],
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, strategy.get().as_ref()).await?;
    let indices = Buffer::copy_from(rows);
    let selection = match (roaring, exclude) {
        (true, false) => Selection::IncludeRoaring(rows.iter().copied().collect()),
        (true, true) => Selection::ExcludeRoaring(rows.iter().copied().collect()),
        (false, false) => Selection::IncludeByIndex(StrictSortedBuffer::try_new(indices.clone())?),
        (false, true) => Selection::ExcludeByIndex(StrictSortedBuffer::try_new(indices.clone())?),
    };
    let dtype = layout.dtype().clone();
    let full = collect(
        dtype.clone(),
        builder(&session, &segments, &layout)?
            .prepare()?
            .execute_stream(None)?,
    )
    .await?;
    let expected_indices = if exclude {
        Buffer::from_iter((0..full.len() as u64).filter(|row| rows.binary_search(row).is_err()))
    } else {
        indices
    };
    let expected = full.take(expected_indices.into_array())?;
    let Executor::V2(scan) =
        select_supported(builder(&session, &segments, &layout)?.with_selection(selection))?
    else {
        panic!("selected rows must use the planner-driven executor");
    };
    let actual = collect(dtype, scan.execute_stream()?).await?;
    assert_arrays_eq!(actual, expected, &mut session.create_execution_ctx());
    Ok(())
}

#[rstest]
#[case::empty(&[])]
#[case::one(&[2301])]
#[case::boundaries(&[0, 999, 1000, 1999, 2000, 3999])]
#[case::scattered(&[1, 17, 1002, 2301, 3333])]
#[tokio::test]
async fn shredded_list_selection_matches_v1(#[case] rows: &[u64]) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let lists = ListLayoutStrategy::default()
        .with_elements(chunked_flat())
        .with_offsets(chunked_flat())
        .with_validity(chunked_flat())
        .with_fallback(chunked_flat());
    let strategy = TableStrategy::new(chunked_flat(), Arc::new(lists));
    let (segments, layout) = write(&session, &strategy).await?;
    assert_same_as_v1(&session, || {
        Ok(builder(&session, &segments, &layout)?
            .with_row_indices(StrictSortedBuffer::try_new(Buffer::copy_from(rows))?))
    })
    .await
}

#[tokio::test]
async fn cached_preparation_keeps_selections_independent() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let file = scan_file(&segments, &layout)?.with_caching();
    assert!(PlanRef::ptr_eq(&file.lower()?, &file.clone().lower()?));
    for rows in [vec![0, 1001, 3999], vec![17, 2500], vec![]] {
        let selection = StrictSortedBuffer::try_new(Buffer::from(rows))?;
        assert_same_as_v1(&session, || {
            Ok(builder(&session, &segments, &layout)?
                .with_scan_file(file.clone())
                .with_row_indices(selection.clone()))
        })
        .await?;
    }
    let mut replaced = file.clone();
    replaced.layout = layout.slot(1)?.ok_or_else(|| vortex_err!("missing ints"))?;
    assert_eq!(replaced.lower()?.dtype(), replaced.layout.dtype());
    assert!(!PlanRef::ptr_eq(&file.lower()?, &replaced.lower()?));
    let original_ranges = file.ranges();
    replaced.locations = Arc::from([SegmentLocation {
        offset: 123456,
        length: 7,
        alignment: Alignment::none(),
    }]);
    assert_eq!(
        replaced.ranges().get(&(123456, 7)),
        Some(SegmentId::from(0))
    );
    assert!(original_ranges.get(&(123456, 7)).is_none());
    Ok(())
}

#[rstest]
#[case::unchanged(None)]
#[case::layout(Some("layout"))]
#[case::locations(Some("locations"))]
#[case::segments(Some("segments"))]
#[tokio::test]
async fn dictionary_cache_reuses_values_and_checks_file_identity(
    #[case] replacement: Option<&str>,
    #[values(false, true)] caching: bool,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let ints = layout.slot(1)?.ok_or_else(|| vortex_err!("missing ints"))?;
    let layout = DictLayout::new(Arc::clone(&ints), Arc::clone(&ints)).into_layout();
    let counted = Arc::new(CountingSegments {
        inner: Arc::clone(&segments),
        reads: AtomicUsize::new(0),
    });
    let replaced = Arc::new(CountingSegments {
        inner: segments,
        reads: AtomicUsize::new(0),
    });
    let segments = Arc::clone(&counted) as Arc<dyn SegmentSource>;
    let mut file = scan_file(&segments, &layout)?;
    if caching {
        file = file.with_caching();
    }
    let reads = || counted.reads.load(Ordering::Relaxed) + replaced.reads.load(Ordering::Relaxed);
    for index in 0..3 {
        if index == 2 {
            match replacement {
                Some("layout") => {
                    file.layout =
                        DictLayout::new(Arc::clone(&ints), Arc::clone(&ints)).into_layout();
                }
                Some("locations") => file.locations = file.locations.to_vec().into(),
                Some("segments") => file.segments = Arc::clone(&replaced) as _,
                _ => {}
            }
        }
        let row = u64::try_from(index * CHUNK_ROWS)?;
        let before = reads();
        let Executor::V2(scan) = select_supported(
            builder(&session, &file.segments, &file.layout)?
                .with_scan_file(file.clone())
                .with_row_range(row..row + 1),
        )?
        else {
            panic!("dictionary projection should run on v2");
        };
        let actual = collect(layout.dtype().clone(), scan.execute_stream()?).await?;
        let expected = Buffer::from(vec![i32::try_from(row)?]).into_array();
        assert_arrays_eq!(actual, expected, &mut session.create_execution_ctx());
        let reused = caching && index > 0 && (index < 2 || replacement.is_none());
        assert_eq!(reads() - before, if reused { 1 } else { CHUNKS });
    }
    Ok(())
}

#[rstest]
#[tokio::test]
async fn support_checks_only_selected_chunks_without_caching_partial_answers(
    #[values(false, true)] full_first: bool,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, chunked_flat().as_ref()).await?;
    let mut children = layout.children()?;
    children[1] = new_foreign_layout(
        LayoutId::new("test.unsupported"),
        layout.dtype().clone(),
        CHUNK_ROWS as u64,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let layout = layout
        .as_opt::<Chunked>()
        .ok_or_else(|| vortex_err!("expected chunks"))?
        .with_children(children)
        .into_layout();
    let file = scan_file(&segments, &layout)?.with_caching();
    if full_first {
        assert!(!file.supports_scan(true, &Selection::All, None)?);
    }
    for (rows, supported) in [
        (vec![], true),
        (vec![0], true),
        (vec![1000], false),
        (vec![3999], true),
    ] {
        let selection = Selection::IncludeByIndex(StrictSortedBuffer::try_new(Buffer::from(rows))?);
        assert_eq!(file.supports_scan(true, &selection, None)?, supported);
    }
    assert!(!file.supports_scan(true, &Selection::All, None)?);
    Ok(())
}

#[derive(Clone, Debug)]
struct CountedLayoutChildren {
    inner: Arc<dyn LayoutChildren>,
    materialized: Arc<AtomicUsize>,
    encoding_hints: bool,
}

impl LayoutChildren for CountedLayoutChildren {
    fn to_arc(&self) -> Arc<dyn LayoutChildren> {
        Arc::new(self.clone())
    }

    fn child(&self, index: usize, dtype: &DType) -> VortexResult<LayoutRef> {
        self.materialized.fetch_add(1, Ordering::Relaxed);
        self.inner.child(index, dtype)
    }

    fn child_row_count(&self, index: usize) -> u64 {
        self.inner.child_row_count(index)
    }

    fn nchildren(&self) -> usize {
        self.inner.nchildren()
    }

    fn child_encoding(&self, index: usize) -> Option<LayoutEncodingRef> {
        self.encoding_hints
            .then(|| self.inner.child_encoding(index))
            .flatten()
    }
}

#[rstest]
#[tokio::test]
async fn caches_whole_subtree_support_without_reusing_replaced_layouts(
    #[values(false, true)] caching: bool,
    #[values(false, true)] unsupported: bool,
    #[values(false, true)] list: bool,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let ints = layout.slot(1)?.ok_or_else(|| vortex_err!("missing ints"))?;
    let mut children = ints.children()?;
    if unsupported {
        children[1] = new_foreign_layout(
            LayoutId::new("test.unsupported"),
            ints.dtype().clone(),
            CHUNK_ROWS as u64,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
    }
    let values = ints.as_::<Chunked>().with_children(children);
    let visited = Arc::new(AtomicUsize::new(0));
    let values = ChunkedLayout::new(
        values.row_count(),
        values.dtype().clone(),
        Arc::new(CountedLayoutChildren {
            inner: Arc::clone(values.children()),
            materialized: Arc::clone(&visited),
            encoding_hints: false,
        }),
    )
    .into_layout();
    let make_layout = |values: LayoutRef, ints: LayoutRef| {
        if list {
            ListLayout::new(
                DType::List(Arc::new(values.dtype().clone()), false.into()),
                values,
                ints,
                None,
            )
            .into_layout()
        } else {
            DictLayout::new(values, ints).into_layout()
        }
    };
    let layout = make_layout(Arc::clone(&values), Arc::clone(&ints));
    let mut file = scan_file(&segments, &layout)?;
    if caching {
        file = file.with_caching();
    }
    visited.store(0, Ordering::Relaxed);
    let selection = Selection::IncludeRoaring([0, 3000].into_iter().collect());
    assert_eq!(file.supports_scan(true, &selection, None)?, !unsupported);
    let first = visited.load(Ordering::Relaxed);
    assert!(first > 0);
    assert_eq!(file.supports_scan(true, &selection, None)?, !unsupported);
    assert_eq!(
        visited.load(Ordering::Relaxed),
        first * if caching { 1 } else { 2 }
    );

    let before = visited.load(Ordering::Relaxed);
    file.layout = make_layout(values, ints);
    assert_eq!(file.supports_scan(true, &selection, None)?, !unsupported);
    assert_eq!(visited.load(Ordering::Relaxed), before + first);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn support_checks_flat_tags_without_materializing_chunks(
    #[values(false, true)] encoding_hints: bool,
    #[values(false, true)] unsupported: bool,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (_, layout) = write(&session, chunked_flat().as_ref()).await?;
    let mut children = layout.children()?;
    if unsupported {
        children[1] = new_foreign_layout(
            LayoutId::new("test.unsupported"),
            layout.dtype().clone(),
            CHUNK_ROWS as u64,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
    }
    let layout = layout
        .as_::<Chunked>()
        .with_children(children)
        .into_layout();
    let context = LayoutContext::default();
    let bytes = layout
        .flatbuffer_writer(&context)
        .write_flatbuffer_bytes()?;
    let viewed = layout_from_flatbuffer_with_options(
        bytes,
        layout.dtype(),
        &ReadContext::new(context.to_ids()),
        &ReadContext::new([]),
        &session,
        true,
    )?;
    let materialized = Arc::new(AtomicUsize::new(0));
    let counted = ChunkedLayout::new(
        viewed.row_count(),
        viewed.dtype().clone(),
        Arc::new(CountedLayoutChildren {
            inner: Arc::clone(viewed.as_::<Chunked>().children()),
            materialized: Arc::clone(&materialized),
            encoding_hints,
        }),
    )
    .into_layout();
    assert_eq!(super::supports_layout(&counted, true)?, !unsupported);
    assert_eq!(
        materialized.load(Ordering::Relaxed),
        match (encoding_hints, unsupported) {
            (true, false) => 0,
            (true, true) => 1,
            (false, false) => CHUNKS,
            (false, true) => 2,
        }
    );
    Ok(())
}

#[rstest]
#[case::empty(vec![])]
#[case::null_only(vec![3, 6, 9])]
#[case::contiguous(vec![1, 2])]
#[case::contiguous_with_empty(vec![7, 8])]
#[case::mixed(vec![0, 1, 2, 3, 8, 31, 63])]
#[case::all((0..64).collect())]
#[tokio::test]
async fn selected_list_values_match_input(
    #[case] rows: Vec<u64>,
    #[values(false, true)] nested: bool,
    #[values(false, true)] nullable: bool,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let mut offsets = vec![0_u32];
    let mut total = 0;
    for row in 0..64 {
        total += row % 4;
        offsets.push(total);
    }
    let inner_strategy = ListLayoutStrategy::default()
        .with_elements(chunked_flat())
        .with_offsets(chunked_flat())
        .with_validity(chunked_flat());
    let (elements, element_strategy) = if nested {
        let elements = ListArray::try_new(
            Buffer::from_iter(0..2 * total).into_array(),
            Buffer::from_iter((0..=total).map(|row| 2 * row)).into_array(),
            Validity::Array(BoolArray::from_iter((0..total).map(|row| row % 3 != 0)).into_array()),
        )?
        .into_array();
        (
            elements,
            Arc::new(inner_strategy) as Arc<dyn LayoutStrategy>,
        )
    } else {
        (Buffer::from_iter(0..total).into_array(), chunked_flat())
    };
    let validity = if nullable {
        Validity::Array(BoolArray::from_iter((0..64).map(|row| row % 3 != 0)).into_array())
    } else {
        Validity::NonNullable
    };
    let input =
        ListArray::try_new(elements, Buffer::from(offsets).into_array(), validity)?.into_array();
    let dtype = input.dtype().clone();
    let expected = input.take(Buffer::from(rows.clone()).into_array())?;
    let strategy = ListLayoutStrategy::default()
        .with_elements(element_strategy)
        .with_offsets(chunked_flat())
        .with_validity(chunked_flat());
    let segments = Arc::new(TestSegments::default());
    let (mut id, eof) = SequenceId::root().split();
    let layout = strategy
        .write_stream(
            ArrayContext::empty().into(),
            segments.clone(),
            SequentialStreamAdapter::new(dtype.clone(), stream::iter([Ok((id.advance(), input))]))
                .sendable(),
            eof,
            &session,
        )
        .await?;
    let counted = Arc::new(CountingSegments {
        inner: segments,
        reads: AtomicUsize::new(0),
    });
    let segments: Arc<dyn SegmentSource> = counted.clone();
    let empty = rows.is_empty();
    let null_only = nullable && !empty && rows.iter().all(|row| row % 3 == 0);
    let Executor::V2(scan) = select_supported(
        builder(&session, &segments, &layout)?
            .with_row_indices(StrictSortedBuffer::try_new(Buffer::from(rows))?),
    )?
    else {
        panic!("list selections must use the planner-driven executor");
    };
    let actual = collect(dtype, scan.execute_stream()?).await?;
    assert_arrays_eq!(actual, expected, &mut session.create_execution_ctx());
    if empty {
        assert_eq!(counted.reads.load(Ordering::Relaxed), 0);
    } else if null_only {
        // Only offsets and validity are needed, even when null lists store nonempty elements.
        assert_eq!(counted.reads.load(Ordering::Relaxed), 2);
    }
    Ok(())
}

#[rstest]
#[tokio::test]
async fn projection_matches_v1(
    #[values(Strategy::Flat, Strategy::Table, Strategy::ZonedTable)] strategy: Strategy,
    #[values(false, true)] selected: bool,
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
        let builder = builder(&session, &segments, &layout)?
            .with_projection(projection.bind(&dtype)?)
            .with_row_offset(100);
        Ok(if selected {
            builder
                .with_row_range(900..2600)
                .with_row_indices(StrictSortedBuffer::try_new(Buffer::from(vec![
                    0, 999, 1000, 2500, 3999,
                ]))?)
        } else {
            builder
        })
    })
    .await
}

#[tokio::test]
async fn zoned_projection_keeps_data_splits() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, zoned_table().as_ref()).await?;
    let projection = get_item("ints", root()).bind(layout.dtype())?;
    let Executor::V2(scan) = select_supported(
        builder(&session, &segments, &layout)?
            .with_projection(projection)
            .with_row_range(250..3100),
    )?
    else {
        panic!("the scan should run on the planner-driven executor");
    };
    assert_eq!(
        scan.splits,
        vec![250..1000, 1000..2000, 2000..3000, 3000..3100]
    );
    Ok(())
}

#[rstest]
#[case::layout(SplitBy::Layout, vec![0..1000, 1000..2000, 2000..3000, 3000..4000])]
#[case::bounded(
    SplitBy::LayoutSubSplitting { max_rows: 600 },
    vec![0..500, 500..1000, 1000..1500, 1500..2000, 2000..2500, 2500..3000, 3000..3500, 3500..4000]
)]
#[case::rows(SplitBy::RowCount(1500), vec![0..1500, 1500..3000, 3000..4000])]
#[tokio::test]
async fn split_policy_is_preserved(
    #[case] policy: SplitBy,
    #[case] expected: Vec<Range<u64>>,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let Executor::V2(scan) =
        select_supported(builder(&session, &segments, &layout)?.with_split_by(policy))?
    else {
        panic!("the scan should run on the planner-driven executor");
    };
    assert_eq!(scan.splits, expected);
    Ok(())
}

#[tokio::test]
async fn supplied_splits_take_precedence_and_are_clamped() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let Executor::V2(scan) = select_supported(
        builder(&session, &segments, &layout)?
            .with_split_by(SplitBy::RowCount(1000))
            .with_natural_splits(Arc::from([0, 2500, 4000]))
            .with_row_range(500..3000),
    )?
    else {
        panic!("the scan should run on the planner-driven executor");
    };
    assert_eq!(scan.splits, vec![500..2500, 2500..3000]);
    Ok(())
}

struct CountingSegments {
    inner: Arc<dyn SegmentSource>,
    reads: AtomicUsize,
}

struct UnalignedSegments {
    inner: Arc<dyn SegmentSource>,
}

impl SegmentSource for UnalignedSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let request = self.inner.request(id);
        async move {
            let bytes = request.await?.try_into_host_sync()?;
            let mut unaligned =
                BufferMut::with_capacity_aligned(bytes.len() + 1, Alignment::of::<u64>());
            unaligned.push(0);
            unaligned.extend_from_slice(&bytes);
            let unaligned = unaligned.freeze().aligned(Alignment::none()).slice(1..);
            assert!(!Alignment::of::<u64>().is_ptr_aligned(unaligned.as_ptr()));
            Ok(BufferHandle::new_host(unaligned))
        }
        .boxed()
    }
}

#[rstest]
#[tokio::test]
async fn point_projection_preserves_struct_validity_and_shared_fields(
    #[values(false, true)] nullable: bool,
    #[values(false, true)] empty: bool,
    #[values(0, 1)] row: usize,
    #[values(false, true)] unaligned: bool,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let segments = Arc::new(TestSegments::default());
    let values = Buffer::from_iter(0i32..20).into_array();
    let validity = BoolArray::from_iter((0..20).map(|index| index % 2 == 0)).into_array();
    let mut layouts = Vec::new();
    for array in [validity.clone(), values.clone()] {
        let (mut sequence, eof) = SequenceId::root().split();
        let dtype = array.dtype().clone();
        layouts.push(
            FlatLayoutStrategy::default()
                .write_stream(
                    ArrayContext::empty().into(),
                    Arc::<TestSegments>::clone(&segments),
                    SequentialStreamAdapter::new(
                        dtype,
                        stream::iter([Ok((sequence.advance(), array))]),
                    )
                    .sendable(),
                    eof,
                    &session,
                )
                .await?,
        );
    }
    let names = if empty {
        FieldNames::default()
    } else {
        FieldNames::from(["left", "right"])
    };
    let fields = if empty {
        vec![]
    } else {
        vec![values.clone(), values]
    };
    let expected = StructArray::try_new(
        names,
        fields,
        20,
        if nullable {
            Validity::Array(validity)
        } else {
            Validity::NonNullable
        },
    )?
    .into_array();
    let mut children = Vec::new();
    if nullable {
        children.push(Arc::clone(&layouts[0]));
    }
    if !empty {
        children.extend([Arc::clone(&layouts[1]), Arc::clone(&layouts[1])]);
    }
    let layout = StructLayout::new(20, expected.dtype().clone(), children).into_layout();
    let counted = Arc::new(CountingSegments {
        inner: segments,
        reads: AtomicUsize::new(0),
    });
    let source = Arc::clone(&counted) as Arc<dyn SegmentSource>;
    let source = if unaligned {
        Arc::new(UnalignedSegments { inner: source }) as Arc<dyn SegmentSource>
    } else {
        source
    };
    let Executor::V2(scan) =
        select_supported(builder(&session, &source, &layout)?.with_selection(
            Selection::IncludeRoaring([row as u64].into_iter().collect()),
        ))?
    else {
        panic!("point projection should run on v2");
    };
    let actual = collect(expected.dtype().clone(), scan.execute_stream()?).await?;
    assert_arrays_eq!(
        actual,
        expected.slice(row..row + 1)?,
        &mut session.create_execution_ctx()
    );
    assert_eq!(
        counted.reads.load(Ordering::Relaxed),
        usize::from(nullable) + usize::from(!empty)
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn wide_point_projection_preserves_field_order_and_deduplicates_reads(
    #[values(15, 16, 17, 64)] unique: usize,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let segments = Arc::new(TestSegments::default());
    let mut fields = Vec::new();
    let mut layouts = Vec::new();
    for field in 0..unique {
        let values = Buffer::from_iter((0..20u64).map(|row| field as u64 * 100 + row)).into_array();
        let (mut sequence, eof) = SequenceId::root().split();
        layouts.push(
            FlatLayoutStrategy::default()
                .write_stream(
                    ArrayContext::empty().into(),
                    Arc::<TestSegments>::clone(&segments),
                    SequentialStreamAdapter::new(
                        values.dtype().clone(),
                        stream::iter([Ok((sequence.advance(), values.clone()))]),
                    )
                    .sendable(),
                    eof,
                    &session,
                )
                .await?,
        );
        fields.push(values);
    }
    fields.extend(fields.clone().into_iter().rev());
    layouts.extend(layouts.clone().into_iter().rev());
    let names = (0..fields.len())
        .map(|index| format!("field_{index}"))
        .collect::<FieldNames>();
    let expected = StructArray::try_new(names, fields, 20, Validity::NonNullable)?.into_array();
    let layout = StructLayout::new(20, expected.dtype().clone(), layouts).into_layout();
    let counted = Arc::new(CountingSegments {
        inner: segments,
        reads: AtomicUsize::new(0),
    });
    let source = Arc::clone(&counted) as Arc<dyn SegmentSource>;
    let Executor::V2(scan) = select_supported(
        builder(&session, &source, &layout)?
            .with_selection(Selection::IncludeRoaring([13].into_iter().collect())),
    )?
    else {
        panic!("wide point projection should run on v2");
    };
    let actual = collect(expected.dtype().clone(), scan.execute_stream()?).await?;
    assert_arrays_eq!(
        actual,
        expected.slice(13..14)?,
        &mut session.create_execution_ctx()
    );
    assert_eq!(counted.reads.load(Ordering::Relaxed), unique);
    Ok(())
}

struct FirstSegmentOnly {
    inner: Arc<dyn SegmentSource>,
    first: SegmentId,
}

impl SegmentSource for FirstSegmentOnly {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        if id == self.first {
            self.inner.request(id)
        } else {
            std::future::pending().boxed()
        }
    }
}

#[tokio::test]
async fn maps_ready_projection_batches_before_later_reads_finish() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, chunked_flat().as_ref()).await?;
    let first = layout
        .slot(0)?
        .ok_or_else(|| vortex_err!("missing first chunk"))?;
    let first = *first
        .segment_ids()
        .first()
        .ok_or_else(|| vortex_err!("missing first segment"))?;
    let segments = Arc::new(FirstSegmentOnly {
        inner: segments,
        first,
    }) as Arc<dyn SegmentSource>;
    let Executor::V2(mut scan) =
        select_supported(builder(&session, &segments, &layout)?.with_selection(
            Selection::IncludeRoaring([0, CHUNK_ROWS as u64].into_iter().collect()),
        ))?
    else {
        panic!("selected projection should run on v2");
    };
    scan.splits = vec![0..2 * CHUNK_ROWS as u64];
    let split = scan
        .split_plans()
        .pop()
        .ok_or_else(|| vortex_err!("missing split"))?;
    let result = split
        .run(Arc::clone(&scan.io), |array| {
            assert_eq!(array.len(), 1);
            Err::<ArrayRef, _>(vortex_err!("mapper received the first selected row"))
        })
        .now_or_never()
        .ok_or_else(|| vortex_err!("mapper waited for later chunks"))?;
    let error = result.expect_err("the mapper must fail before the pending read completes");
    assert!(
        error
            .to_string()
            .contains("mapper received the first selected row")
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
async fn missing_segment_location_is_rejected_before_registering_reads() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let counted = Arc::new(CountingSegments {
        inner: segments,
        reads: AtomicUsize::new(0),
    });
    let segments = Arc::clone(&counted) as Arc<dyn SegmentSource>;
    let mut scan = builder(&session, &segments, &layout)?
        .with_selection(Selection::IncludeRoaring([0].into_iter().collect()));
    scan.scan_file
        .as_mut()
        .ok_or_else(|| vortex_err!("missing scan file"))?
        .locations = Arc::from([]);
    let error = match select_supported(scan) {
        Err(error) => error,
        Ok(_) => panic!("missing segment location must be rejected"),
    };
    assert!(error.to_string().contains("has no known location"));
    assert_eq!(counted.reads.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn dictionary_values_are_reused_between_splits() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let ints = layout.slot(1)?.ok_or_else(|| vortex_err!("missing ints"))?;
    // The codes 0..4000 index the same values, so each split returns its own row numbers.
    let layout = DictLayout::new(Arc::clone(&ints), ints).into_layout();
    let counted = Arc::new(CountingSegments {
        inner: segments,
        reads: AtomicUsize::new(0),
    });
    let segments = Arc::clone(&counted) as Arc<dyn SegmentSource>;
    let Executor::V2(scan) = select_supported(builder(&session, &segments, &layout)?)? else {
        panic!("the scan should run on the planner-driven executor");
    };
    let splits = scan.split_plans();
    assert_eq!(splits.len(), CHUNKS);
    for (index, split) in splits.into_iter().enumerate() {
        let before = counted.reads.load(Ordering::Relaxed);
        let arrays = split.run(Arc::clone(&scan.io), Ok).await?;
        let reads = counted.reads.load(Ordering::Relaxed) - before;
        assert_eq!(reads, if index == 0 { CHUNKS } else { 1 });
        let start = i32::try_from(index * CHUNK_ROWS)?;
        let expected = PrimitiveArray::from_iter(start..start + i32::try_from(CHUNK_ROWS)?);
        assert_arrays_eq!(
            ChunkedArray::try_new(arrays, layout.dtype().clone())?.into_array(),
            expected,
            &mut session.create_execution_ctx()
        );
    }
    Ok(())
}

#[tokio::test]
async fn filters_and_limits_run_on_v1() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let dtype = layout.dtype().clone();
    let filter = gt(get_item("ints", root()), lit(10i32)).bind(&dtype)?;
    let filtered = builder(&session, &segments, &layout)?.with_filter(filter);
    assert!(matches!(select_supported(filtered)?, Executor::V1(_)));
    let limited = builder(&session, &segments, &layout)?.with_limit(10);
    assert!(matches!(select_supported(limited)?, Executor::V1(_)));
    Ok(())
}

#[tokio::test]
async fn scans_without_a_scan_file_run_on_v1() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let reader = layout.new_reader(
        "".into(),
        Arc::clone(&segments),
        &session,
        &Default::default(),
    )?;
    let builder = ScanBuilder::new(session.clone(), reader);
    assert!(matches!(select_supported(builder)?, Executor::V1(_)));
    Ok(())
}

#[tokio::test]
async fn list_layouts_match_v1() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let strategy = TableStrategy::new(chunked_flat(), chunked_flat()).with_list_layout();
    let (segments, layout) = write(&session, &strategy).await?;
    assert_same_as_v1(&session, || builder(&session, &segments, &layout)).await
}

#[tokio::test]
async fn unordered_returns_every_row() -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, table().as_ref()).await?;
    let Executor::V2(scan) =
        select_supported(builder(&session, &segments, &layout)?.with_ordered(false))?
    else {
        panic!("the scan should run on the planner-driven executor");
    };
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
    let Executor::V2(scan) = select_supported(
        builder(&session, &segments, &layout)?
            .with_projection(get_item("ints", root()).bind(&dtype)?)
            .with_row_range(990..1010),
    )?
    else {
        panic!("the scan should run on the planner-driven executor");
    };
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

/// Every chunk of the default zoned table is its own split, so the scan runs them in parallel.
#[rstest]
#[tokio::test]
async fn splits_at_chunk_starts(
    #[values(Strategy::Flat, Strategy::Table, Strategy::ZonedTable)] strategy: Strategy,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let (segments, layout) = write(&session, strategy.get().as_ref()).await?;
    let Executor::V2(scan) = select_supported(builder(&session, &segments, &layout)?)? else {
        panic!("the scan should run on the planner-driven executor");
    };
    let expected = (0..CHUNKS as u64)
        .map(|chunk| chunk * CHUNK_ROWS as u64..(chunk + 1) * CHUNK_ROWS as u64)
        .collect::<Vec<_>>();
    assert_eq!(scan.splits, expected);
    Ok(())
}
