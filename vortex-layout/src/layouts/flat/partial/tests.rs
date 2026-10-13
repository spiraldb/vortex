// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![allow(clippy::cast_possible_truncation, clippy::single_range_in_vec_init)]

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use rstest::rstest;
use vortex_array::ArrayContext;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::MaskFuture;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::DictArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::VarBinArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::assert_arrays_eq;
use vortex_array::dtype::FieldNames;
use vortex_array::expr::get_item;
use vortex_array::expr::gt;
use vortex_array::expr::lit;
use vortex_array::expr::root;
use vortex_array::validity::Validity;
use vortex_buffer::Alignment;
use vortex_error::VortexResult;
use vortex_io::runtime::single::block_on;
use vortex_io::session::RuntimeSessionExt;
use vortex_mask::Mask;

use crate::LayoutStrategy;
use crate::layouts::flat::Flat;
use crate::layouts::flat::FlatLayout;
use crate::layouts::flat::FlatRangeRead;
use crate::layouts::flat::writer::FlatLayoutStrategy;
use crate::segments::SegmentFuture;
use crate::segments::SegmentId;
use crate::segments::SegmentSource;
use crate::segments::TestSegments;
use crate::sequence::SequenceId;
use crate::sequence::SequentialArrayStreamExt;
use crate::test::new_session;

const N: usize = 100_000;

/// Counts the bytes requested from the inner source.
struct CountingSource {
    inner: TestSegments,
    whole_segments: AtomicUsize,
    range_bytes: AtomicUsize,
}

impl SegmentSource for CountingSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.whole_segments.fetch_add(1, Ordering::Relaxed);
        self.inner.request(id)
    }

    fn request_range(
        &self,
        id: SegmentId,
        range: Range<usize>,
        alignment: Alignment,
    ) -> SegmentFuture {
        self.range_bytes.fetch_add(range.len(), Ordering::Relaxed);
        self.inner.request_range(id, range, alignment)
    }
}

fn primitive() -> ArrayRef {
    PrimitiveArray::from_iter((0..N as i64).map(|i| i * 3)).into_array()
}

fn nullable_primitive() -> ArrayRef {
    PrimitiveArray::from_option_iter((0..N as i32).map(|i| (i % 7 != 0).then_some(i))).into_array()
}

fn bools() -> ArrayRef {
    BoolArray::from_iter((0..N).map(|i| i % 3 == 0)).into_array()
}

fn strings() -> ArrayRef {
    VarBinViewArray::from_iter_str((0..N).map(|i| format!("a fairly long string value {i}")))
        .into_array()
}

fn varbin() -> ArrayRef {
    VarBinArray::from_iter(
        (0..N).map(|i| Some(format!("string {i}"))),
        vortex_array::dtype::DType::Utf8(vortex_array::dtype::Nullability::Nullable),
    )
    .into_array()
}

fn dict() -> ArrayRef {
    let codes = PrimitiveArray::from_iter((0..N).map(|i| (i % 100) as u32)).into_array();
    let values = PrimitiveArray::from_iter((0..100).map(|i| i as f64 * 1.5)).into_array();
    DictArray::try_new(codes, values)
        .expect("valid dict")
        .into_array()
}

fn struct_() -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["a", "b"]),
        vec![primitive(), strings()],
        N,
        Validity::NonNullable,
    )
    .expect("valid struct")
    .into_array()
}

async fn write_flat(array: &ArrayRef, segments: &Arc<TestSegments>) -> VortexResult<FlatLayout> {
    let session = new_session();
    let (ptr, eof) = SequenceId::root().split();
    let layout = FlatLayoutStrategy::default()
        .with_inline_array_tree(true)
        .write_stream(
            ArrayContext::empty().into(),
            Arc::<TestSegments>::clone(segments),
            array.to_array_stream().sequenced(ptr),
            eof,
            &session,
        )
        .await?;
    Ok(layout.as_::<Flat>().clone())
}

fn counting(segments: &Arc<TestSegments>) -> Arc<CountingSource> {
    Arc::new(CountingSource {
        inner: segments.as_ref().clone(),
        whole_segments: AtomicUsize::new(0),
        range_bytes: AtomicUsize::new(0),
    })
}

#[rstest]
#[case::primitive(primitive())]
#[case::nullable_primitive(nullable_primitive())]
#[case::bools(bools())]
#[case::strings(strings())]
#[case::dict(dict())]
#[case::struct_(struct_())]
fn reads_rows_without_whole_segment(#[case] array: ArrayRef) -> VortexResult<()> {
    block_on(|handle| async move {
        let session = new_session().with_handle(handle);
        let mut ctx = session.create_execution_ctx();
        let segments = Arc::new(TestSegments::default());
        let layout = write_flat(&array, &segments).await?;
        let source = counting(&segments);

        let row_ranges = vec![1234..1235, 5000..5010, N - 1..N];
        let read = FlatRangeRead::try_new(&layout, row_ranges.clone(), &session)?;
        let segment_bytes = read.segment_data_nbytes();
        let rows = read
            .read(Arc::<CountingSource>::clone(&source))
            .await?
            .expect("partial read succeeds");

        assert_eq!(source.whole_segments.load(Ordering::Relaxed), 0);
        assert_eq!(source.range_bytes.load(Ordering::Relaxed), rows.nbytes());
        assert!(
            rows.nbytes() * 20 < segment_bytes,
            "read {} of {segment_bytes} bytes: {:?}",
            rows.nbytes(),
            rows.byte_ranges
        );
        for (rows, result) in row_ranges.iter().zip(rows.arrays) {
            assert_arrays_eq!(result, array.slice(rows.clone())?, &mut ctx);
        }
        Ok(())
    })
}

/// Decoding a `VarBin` array reads its bytes, and slicing it reads its offsets, each of which
/// takes a round trip.
#[test]
fn reads_data_needed_to_decode_and_slice() -> VortexResult<()> {
    block_on(|handle| async move {
        let session = new_session().with_handle(handle);
        let mut ctx = session.create_execution_ctx();
        let array = varbin();
        let segments = Arc::new(TestSegments::default());
        let layout = write_flat(&array, &segments).await?;

        let rows = FlatRangeRead::try_new(&layout, vec![10..11, 777..780], &session)?
            .read(counting(&segments))
            .await?
            .expect("partial read succeeds");
        assert_eq!(rows.round_trips, 2);
        assert_arrays_eq!(rows.arrays[0], array.slice(10..11)?, &mut ctx);
        assert_arrays_eq!(rows.arrays[1], array.slice(777..780)?, &mut ctx);
        Ok(())
    })
}

#[test]
fn gives_up_beyond_max_bytes() -> VortexResult<()> {
    block_on(|handle| async move {
        let session = new_session().with_handle(handle);
        let segments = Arc::new(TestSegments::default());
        let layout = write_flat(&primitive(), &segments).await?;
        let source = counting(&segments);

        let rows = FlatRangeRead::try_new(&layout, vec![0..1000], &session)?
            .with_max_bytes(1000)
            .read(Arc::<CountingSource>::clone(&source))
            .await?;
        assert!(rows.is_none());
        assert_eq!(source.range_bytes.load(Ordering::Relaxed), 0);
        Ok(())
    })
}

#[test]
fn reads_empty_row_ranges() -> VortexResult<()> {
    block_on(|handle| async move {
        let session = new_session().with_handle(handle);
        let segments = Arc::new(TestSegments::default());
        let layout = write_flat(&struct_(), &segments).await?;

        let rows = FlatRangeRead::try_new(&layout, vec![5..5], &session)?
            .read(counting(&segments))
            .await?
            .expect("partial read succeeds");
        assert_eq!(rows.nbytes(), 0);
        assert_eq!(rows.arrays[0].len(), 0);
        Ok(())
    })
}

#[test]
fn requires_inline_array_tree() -> VortexResult<()> {
    block_on(|handle| async move {
        let session = new_session().with_handle(handle);
        let segments = Arc::new(TestSegments::default());
        let (ptr, eof) = SequenceId::root().split();
        let layout = FlatLayoutStrategy::default()
            .with_inline_array_tree(false)
            .write_stream(
                ArrayContext::empty().into(),
                Arc::<TestSegments>::clone(&segments),
                primitive().to_array_stream().sequenced(ptr),
                eof,
                &session,
            )
            .await?;
        let layout = layout.as_::<Flat>().clone();
        assert!(FlatRangeRead::try_new(&layout, vec![0..1], &session).is_err());
        Ok(())
    })
}

/// The flat reader reads only the selected rows when the mask is known up front.
#[rstest]
#[case::point(Mask::from_indices(N, [4321]))]
#[case::scattered(Mask::from_indices(N, [3, 30_000, 60_001, N - 1]))]
#[case::run(Mask::from_slices(N, vec![(100, 200)]))]
fn reader_reads_selected_rows(#[case] mask: Mask) -> VortexResult<()> {
    block_on(|handle| async move {
        let session = new_session().with_handle(handle);
        let mut ctx = session.create_execution_ctx();
        let array = struct_();
        let segments = Arc::new(TestSegments::default());
        let layout = write_flat(&array, &segments).await?;
        let source = counting(&segments);

        let reader = layout.new_reader(
            "".into(),
            Arc::<CountingSource>::clone(&source) as Arc<dyn SegmentSource>,
            &session,
            &Default::default(),
        )?;
        let projection = get_item("b", root()).bind(reader.dtype())?;
        let result = reader
            .projection_evaluation(&(0..N as u64), &projection, MaskFuture::ready(mask.clone()))?
            .await?;
        let expected = array.filter(mask.clone())?.apply(&get_item("b", root()))?;
        assert_arrays_eq!(result, expected, &mut ctx);

        let filter = gt(get_item("a", root()), lit(3000i64)).bind(reader.dtype())?;
        let result = reader
            .filter_evaluation(&(0..N as u64), &filter, MaskFuture::ready(mask.clone()))?
            .await?;
        let expected = Mask::from_indices(
            N,
            mask.iter()
                .enumerate()
                .filter(|&(idx, selected)| selected && idx * 3 > 3000)
                .map(|(idx, _)| idx),
        );
        assert_eq!(result, expected);

        assert_eq!(source.whole_segments.load(Ordering::Relaxed), 0);
        Ok(())
    })
}

/// A mask whose rows touch most blocks reads the whole segment.
#[rstest]
#[case::dense(Mask::from_slices(N, vec![(0, N - 1)]))]
#[case::scattered(Mask::from_indices(N, (0..N).step_by(997)))]
fn reader_reads_whole_segment_for_dense_mask(#[case] mask: Mask) -> VortexResult<()> {
    block_on(|handle| async move {
        let session = new_session().with_handle(handle);
        let mut ctx = session.create_execution_ctx();
        let array = primitive();
        let segments = Arc::new(TestSegments::default());
        let layout = write_flat(&array, &segments).await?;
        let source = counting(&segments);

        let reader = layout.new_reader(
            "".into(),
            Arc::<CountingSource>::clone(&source) as Arc<dyn SegmentSource>,
            &session,
            &Default::default(),
        )?;
        let result = reader
            .projection_evaluation(
                &(0..N as u64),
                &root().bind(reader.dtype())?,
                MaskFuture::ready(mask.clone()),
            )?
            .await?;
        assert_arrays_eq!(result, array.filter(mask)?, &mut ctx);
        assert_eq!(source.whole_segments.load(Ordering::Relaxed), 1);
        Ok(())
    })
}

#[test]
fn segment_bytes_get_serves_only_read_ranges() {
    use vortex_buffer::ByteBuffer;

    use super::SegmentBytes;

    let bytes = SegmentBytes::default();
    bytes.insert(vec![
        (
            10..20,
            ByteBuffer::copy_from((10u8..20).collect::<Vec<_>>()),
        ),
        (
            20..25,
            ByteBuffer::copy_from((20u8..25).collect::<Vec<_>>()),
        ),
        (
            30..40,
            ByteBuffer::copy_from((30u8..40).collect::<Vec<_>>()),
        ),
    ]);

    let get = |range: Range<usize>| bytes.get(&range).map(|b| b.as_slice().to_vec());
    assert_eq!(get(12..15), Some(vec![12, 13, 14]));
    // Adjacent reads are copied together.
    assert_eq!(get(18..23), Some(vec![18, 19, 20, 21, 22]));
    // Ranges with unread bytes are not served.
    assert_eq!(get(23..31), None);
    assert_eq!(get(5..12), None);
    assert_eq!(get(38..41), None);
    assert_eq!(get(0..0), Some(vec![]));
}
