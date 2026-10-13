// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Point lookups in files whose flat layouts carry their array tree inline read only the bytes of
//! each segment that the selected rows need.

#![expect(clippy::tests_outside_test_module)]
#![allow(clippy::cast_possible_truncation, clippy::single_range_in_vec_init)]

use std::ops::Range;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::assert_arrays_eq;
use vortex_array::dtype::FieldNames;
use vortex_array::stream::ArrayStreamExt;
use vortex_array::validity::Validity;
use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_file::OpenOptionsSessionExt;
use vortex_file::WriteOptionsSessionExt;
use vortex_file::WriteStrategyBuilder;
use vortex_io::session::RuntimeSession;
use vortex_layout::layouts::flat::Flat;
use vortex_layout::layouts::flat::FlatRangeRead;
use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex_layout::segments::SegmentFuture;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;
use vortex_layout::session::LayoutSession;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_session::VortexSession;

mod common;

use common::enable_all_registered_array_encodings;

const N: usize = 200_000;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session()
        .with::<LayoutSession>()
        .with::<RuntimeSession>();
    vortex_file::register_default_encodings(&session);
    enable_all_registered_array_encodings(&session);
    session
});

/// Counts the bytes read through a segment source.
struct CountingSource {
    inner: Arc<dyn SegmentSource>,
    bytes: Arc<AtomicUsize>,
}

impl SegmentSource for CountingSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let bytes = Arc::clone(&self.bytes);
        self.inner
            .request(id)
            .inspect(move |r| {
                if let Ok(buffer) = r {
                    bytes.fetch_add(buffer.len(), Ordering::Relaxed);
                }
            })
            .boxed()
    }

    fn request_range(
        &self,
        id: SegmentId,
        range: Range<usize>,
        alignment: Alignment,
    ) -> SegmentFuture {
        self.bytes.fetch_add(range.len(), Ordering::Relaxed);
        self.inner.request_range(id, range, alignment)
    }
}

/// Columns whose encodings can read single rows: integers, floats, booleans and dictionaries.
fn numeric_columns() -> Vec<(&'static str, ArrayRef)> {
    vec![
        (
            "ints",
            PrimitiveArray::from_iter((0..N as i64).map(|i| 1_000_000 + i * 7)).into_array(),
        ),
        (
            "noise",
            PrimitiveArray::from_iter((0..N as u32).map(|i| i.wrapping_mul(2_654_435_761) % 5000))
                .into_array(),
        ),
        (
            "floats",
            PrimitiveArray::from_iter((0..N).map(|i| (i % 10_000) as f64 * 0.25 + 3.5))
                .into_array(),
        ),
        (
            "nullable",
            PrimitiveArray::from_option_iter(
                (0..N as i32).map(|i| (i % 5 != 0).then_some(i % 1000)),
            )
            .into_array(),
        ),
        (
            "runs",
            PrimitiveArray::from_iter((0..N).map(|i| (i / 1000) as u16)).into_array(),
        ),
        (
            "bools",
            BoolArray::from_iter((0..N).map(|i| i % 3 == 0)).into_array(),
        ),
        (
            "categories",
            VarBinViewArray::from_iter_str(
                (0..N).map(|i| ["alpha", "beta", "gamma", "delta", "epsilon"][i % 5]),
            )
            .into_array(),
        ),
    ]
}

/// High-cardinality strings, which compress with FSST or OnPair. Those encodings decode or slice
/// over all of their compressed bytes, so reading a row from them reads most of the segment.
fn string_columns() -> Vec<(&'static str, ArrayRef)> {
    vec![
        (
            "urls",
            VarBinViewArray::from_iter_str((0..N).map(|i| {
                format!(
                    "https://example.com/items/{i}/details?ref=search&page={}",
                    i % 97
                )
            }))
            .into_array(),
        ),
        (
            "uuids",
            VarBinViewArray::from_iter_str((0..N).map(|i| {
                format!(
                    "{:032x}",
                    (i as u128).wrapping_mul(0x9e37_79b9_7f4a_7c15_f39c)
                )
            }))
            .into_array(),
        ),
    ]
}

fn table(columns: Vec<(&'static str, ArrayRef)>) -> VortexResult<ArrayRef> {
    let (names, fields): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    Ok(
        StructArray::try_new(FieldNames::from(names), fields, N, Validity::NonNullable)?
            .into_array(),
    )
}

fn all_columns() -> VortexResult<ArrayRef> {
    table(
        numeric_columns()
            .into_iter()
            .chain(string_columns())
            .collect(),
    )
}

async fn write(array: &ArrayRef, inline_array_tree: bool) -> VortexResult<tempfile::TempPath> {
    let strategy = WriteStrategyBuilder::from_session(&SESSION)
        .with_flat_strategy(Arc::new(
            FlatLayoutStrategy::default().with_inline_array_tree(inline_array_tree),
        ))
        .build();
    let path = tempfile::NamedTempFile::new()?.into_temp_path();
    let mut file = tokio::fs::File::create(&path).await?;
    SESSION
        .write_options()
        .with_strategy(strategy)
        .write(&mut file, array.to_array_stream())
        .await?;
    Ok(path)
}

/// Read `indices` from the file, returning the rows and the number of bytes read.
async fn take(path: &tempfile::TempPath, indices: &[u64]) -> VortexResult<(ArrayRef, usize)> {
    let file = SESSION.open_options().open_path(path).await?;
    let bytes = Arc::new(AtomicUsize::new(0));
    let inner = file.segment_source();
    let file = file.with_segment_source(Arc::new(CountingSource {
        inner,
        bytes: Arc::clone(&bytes),
    }));
    let rows = file
        .scan()?
        .with_row_indices(StrictSortedBuffer::try_new(Buffer::from_iter(
            indices.iter().copied(),
        ))?)
        .into_array_stream()?
        .read_all()
        .await?;
    Ok((rows, bytes.load(Ordering::Relaxed)))
}

/// Read `indices` from files written with and without inline array trees, checking the rows, and
/// returning the bytes read from each.
async fn compare_reads(table: &ArrayRef, indices: &[u64]) -> VortexResult<(usize, usize)> {
    let mut ctx = SESSION.create_execution_ctx();
    let expected = table.take(PrimitiveArray::from_iter(indices.iter().copied()).into_array())?;

    let whole = write(table, false).await?;
    let (rows, whole_bytes) = take(&whole, indices).await?;
    assert_arrays_eq!(rows, expected, &mut ctx);

    let inline = write(table, true).await?;
    let (rows, partial_bytes) = take(&inline, indices).await?;
    assert_arrays_eq!(rows, expected, &mut ctx);

    eprintln!(
        "{} rows: {partial_bytes} bytes, {whole_bytes} without partial reads",
        indices.len()
    );
    Ok((partial_bytes, whole_bytes))
}

#[rstest]
#[case::point(vec![123_457])]
#[case::few(vec![3, 70_001, 150_000, 199_999])]
#[case::run((100_000..100_064).collect())]
#[tokio::test]
async fn point_lookups_read_few_bytes(#[case] indices: Vec<u64>) -> VortexResult<()> {
    let (partial_bytes, whole_bytes) = compare_reads(&table(numeric_columns())?, &indices).await?;
    assert!(partial_bytes * 20 < whole_bytes);
    Ok(())
}

#[rstest]
#[case::point(vec![123_457])]
#[case::few(vec![3, 70_001, 150_000, 199_999])]
#[tokio::test]
async fn point_lookups_read_strings(#[case] indices: Vec<u64>) -> VortexResult<()> {
    let (partial_bytes, whole_bytes) = compare_reads(&all_columns()?, &indices).await?;
    assert!(partial_bytes < whole_bytes);
    Ok(())
}

/// Every flat layout in the file can read a single row from a fraction of its segment.
#[tokio::test]
async fn every_column_reads_part_of_its_segments() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let path = write(&all_columns()?, true).await?;
    let file = SESSION.open_options().open_path(&path).await?;
    let segment_source = file.segment_source();

    for layout in file.footer().layout().depth_first_traversal() {
        let layout = layout?;
        let Some(flat) = layout.as_opt::<Flat>() else {
            continue;
        };
        let flat = flat.clone();
        let row = usize::try_from(flat.row_count())? / 2;

        let read = FlatRangeRead::try_new(&flat, vec![row..row + 1], &SESSION)?;
        let segment_bytes = read.segment_data_nbytes();
        let Some(rows) = read.read(Arc::clone(&segment_source)).await? else {
            eprintln!("{:>10} read whole segment", flat.dtype().to_string());
            continue;
        };

        let whole = flat
            .new_reader(
                "".into(),
                Arc::clone(&segment_source),
                &SESSION,
                &Default::default(),
            )?
            .projection_evaluation(
                &(0..flat.row_count()),
                &vortex_array::expr::root().bind(flat.dtype())?,
                vortex_array::MaskFuture::new_true(usize::try_from(flat.row_count())?),
            )?
            .await?;
        eprintln!(
            "{:>10} {:<40} {:>8} of {:>8} bytes in {} round trips",
            flat.dtype().to_string(),
            whole.encoding_id().to_string(),
            rows.nbytes(),
            segment_bytes,
            rows.round_trips,
        );
        assert_arrays_eq!(rows.arrays[0], whole.slice(row..row + 1)?, &mut ctx);
    }
    Ok(())
}

/// Columns that compress with patches: bit-packed integers with rare large outliers, and ALP floats
/// with values that have no short decimal representation.
fn patched_columns() -> Vec<(&'static str, ArrayRef)> {
    vec![
        (
            "outliers",
            PrimitiveArray::from_iter(
                (0..N as u32).map(|i| if i % 997 == 0 { 1 << 30 | i } else { i % 100 }),
            )
            .into_array(),
        ),
        (
            "irrational",
            PrimitiveArray::from_iter((0..N).map(|i| {
                if i % 50 == 0 {
                    (i as f64).sqrt()
                } else {
                    (i % 10_000) as f64 * 0.25
                }
            }))
            .into_array(),
        ),
    ]
}

/// Slicing patches searches all of their indices, then reads only the patch values in the range.
#[tokio::test]
async fn patched_columns_read_part_of_their_segments() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let path = write(&table(patched_columns())?, true).await?;
    let file = SESSION.open_options().open_path(&path).await?;
    let segment_source = file.segment_source();

    let mut patched = 0;
    for layout in file.footer().layout().depth_first_traversal() {
        let layout = layout?;
        let Some(flat) = layout.as_opt::<Flat>() else {
            continue;
        };
        let flat = flat.clone();
        if !flat.dtype().is_primitive() {
            continue;
        }
        let whole = flat
            .new_reader(
                "".into(),
                Arc::clone(&segment_source),
                &SESSION,
                &Default::default(),
            )?
            .projection_evaluation(
                &(0..flat.row_count()),
                &vortex_array::expr::root().bind(flat.dtype())?,
                vortex_array::MaskFuture::new_true(usize::try_from(flat.row_count())?),
            )?
            .await?;
        let tree = whole.display_tree_encodings_only().to_string();
        if !tree.contains("patch") {
            continue;
        }
        patched += 1;

        for rows in [0..1, 4321..4322, 10_000..10_064] {
            let read = FlatRangeRead::try_new(&flat, vec![rows.clone()], &SESSION)?;
            let segment_bytes = read.segment_data_nbytes();
            let read = read
                .read(Arc::clone(&segment_source))
                .await?
                .expect("partial read succeeds");
            eprintln!(
                "{} rows {}..{}: {} of {segment_bytes} bytes in {} round trips\n{tree}",
                flat.dtype(),
                rows.start,
                rows.end,
                read.nbytes(),
                read.round_trips,
            );
            assert!(read.nbytes() * 10 < segment_bytes);
            assert_arrays_eq!(read.arrays[0], whole.slice(rows)?, &mut ctx);
        }
    }
    assert!(patched > 0, "expected some layouts with patches");
    Ok(())
}
