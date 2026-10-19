// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

// Fixtures use a 20-row domain and name columns after their struct fields.
#![allow(clippy::cast_possible_truncation, clippy::many_single_char_names)]

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use rstest::rstest;
use vortex_array::ArrayContext;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::List;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::list::ListArraySlotsExt;
use vortex_array::assert_arrays_eq;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::StructFields;
use vortex_array::expr::get_item;
use vortex_array::expr::gt;
use vortex_array::expr::lit;
use vortex_array::expr::root;
use vortex_array::serde::SerializeOptions;
use vortex_array::validity::Validity;
use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;
use vortex_session::registry::CachedId;
use vortex_session::registry::ReadContext;
use vortex_utils::parallelism::get_available_parallelism;

use super::port::SourceEnd;
use super::synthetic::SyntheticPlan;
use super::synthetic::row_dtype;
use super::*;
use crate::LayoutRef;
use crate::OwnedLayoutChildren;
use crate::layouts::chunked::ChunkedLayout;
use crate::layouts::dict::DictLayout;
use crate::layouts::flat::FlatLayout;
use crate::layouts::foreign::new_foreign_layout;
use crate::layouts::struct_::StructLayout;
use crate::plan::physical::ConcatPlan;
use crate::plan::physical::EvalPlan;
use crate::plan::physical::Filter;
use crate::plan::physical::ListPack;
use crate::plan::physical::ListPackPlan;
use crate::plan::physical::PackPlan;
use crate::plan::physical::PlanRef;
use crate::plan::physical::SegmentScan;
use crate::plan::physical::Take;
use crate::plan::physical::lower;
use crate::plan::physical::operator::Cx;
use crate::plan::physical::operator::InletSet;
use crate::plan::physical::operator::Input;
use crate::plan::physical::operator::Operator;
use crate::plan::physical::operator::Source;
use crate::plan::physical::operator::SourceBlocked;
use crate::plan::physical::operator::SourceReady;
use crate::plan::physical::operator::Step;
use crate::plan::physical::operators::PackSource;
use crate::plan::physical::optimize;
use crate::segments::SegmentId;
use crate::test::SESSION;

const ROWS: u64 = 20;

/// In-memory segments, standing in for the IO service.
#[derive(Default)]
struct Store {
    segments: Vec<BufferHandle>,
}

impl Store {
    fn flat(&mut self, array: &ArrayRef) -> VortexResult<LayoutRef> {
        let ctx = ArrayContext::empty();
        let buffers = array.serialize(
            &ctx,
            &SESSION,
            &SerializeOptions {
                offset: 0,
                include_padding: true,
            },
        )?;
        let mut bytes = ByteBufferMut::empty_aligned(Alignment::new(64));
        for buffer in buffers {
            bytes.extend_from_slice(buffer.as_ref());
        }
        let segment_id = SegmentId::from(u32::try_from(self.segments.len())?);
        self.segments.push(BufferHandle::new_host(bytes.freeze()));
        Ok(FlatLayout::new(
            array.len() as u64,
            array.dtype().clone(),
            segment_id,
            ReadContext::new(ctx.to_ids()),
        )
        .into_layout())
    }

    fn chunked(&mut self, array: &ArrayRef, sizes: &[usize]) -> VortexResult<LayoutRef> {
        let mut chunks = Vec::with_capacity(sizes.len());
        let mut start = 0;
        for size in sizes {
            chunks.push(self.flat(&array.slice(start..start + size)?)?);
            start += size;
        }
        assert_eq!(start, array.len(), "chunk sizes must cover the array");
        Ok(ChunkedLayout::new(
            array.len() as u64,
            array.dtype().clone(),
            OwnedLayoutChildren::layout_children(chunks),
        )
        .into_layout())
    }

    fn read(&self, segment_id: SegmentId) -> BufferHandle {
        self.segments[*segment_id as usize].clone()
    }
}

/// A struct whose columns are chunked at different row boundaries.
///
/// ```text
/// a  i32        chunks [0,7) [7,12) [12,20)
/// b  i64        chunks [0,10) [10,20)
/// c  utf8       flat   [0,20)
/// d  i32?       chunks [0,1) [1,3) [3,6) [6,20)
/// e  struct     x: chunks [0,5) [5,20), y: flat
/// ```
fn fixture(store: &mut Store) -> VortexResult<(PlanRef, ArrayRef)> {
    let a = PrimitiveArray::from_iter(0..ROWS as i32).into_array();
    let b = PrimitiveArray::from_iter((0..ROWS as i64).map(|v| v * 10)).into_array();
    let c = VarBinViewArray::from_iter_str((0..ROWS).map(|v| format!("row-{v}"))).into_array();
    let d = PrimitiveArray::from_option_iter((0..ROWS as i32).map(|v| (v % 3 != 0).then_some(v)))
        .into_array();
    let x = PrimitiveArray::from_iter((0..ROWS).map(|v| v as u8)).into_array();
    let y = BoolArray::from_iter((0..ROWS).map(|v| v % 2 == 0)).into_array();
    let e = StructArray::from_fields(&[("x", x.clone()), ("y", y.clone())])?.into_array();
    let expected = StructArray::from_fields(&[
        ("a", a.clone()),
        ("b", b.clone()),
        ("c", c.clone()),
        ("d", d.clone()),
        ("e", e.clone()),
    ])?
    .into_array();

    let e_layout = StructLayout::new(
        ROWS,
        e.dtype().clone(),
        vec![store.chunked(&x, &[5, 15])?, store.flat(&y)?],
    )
    .into_layout();
    let layout = StructLayout::new(
        ROWS,
        expected.dtype().clone(),
        vec![
            store.chunked(&a, &[7, 5, 8])?,
            store.chunked(&b, &[10, 10])?,
            store.flat(&c)?,
            store.chunked(&d, &[1, 2, 3, 14])?,
            e_layout,
        ],
    )
    .into_layout();
    Ok((lower(&layout)?, expected))
}

/// Which outstanding read the scripted IO service completes next.
#[derive(Clone, Copy, Debug)]
enum Delivery {
    /// Oldest read first.
    Fifo,
    /// Newest read first.
    Lifo,
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    /// Reads the scan asked for before it next waited, as segment ids.
    Io(Vec<u32>),
    /// A read delivered for this segment.
    Delivered(u32),
    /// An array of this many rows came out.
    Piece(usize),
}

struct Run {
    /// Every array, in the order it came out.
    arrays: Vec<ArrayRef>,
    /// The arrays of each split, in order.
    splits: Vec<Vec<ArrayRef>>,
    events: Vec<Event>,
    /// Ports and shared readers still held once the scan finished.
    leftover: (usize, usize),
}

/// Drives a scan the way an owner does, completing reads only while the scan waits.
fn drive(
    store: &Store,
    scan: Scan,
    mut pick: impl FnMut(&[ReadRequest]) -> usize,
) -> VortexResult<Run> {
    drive_inner(store, scan, &mut pick)
}

fn drive_inner(
    store: &Store,
    mut scan: Scan,
    pick: &mut dyn FnMut(&[ReadRequest]) -> usize,
) -> VortexResult<Run> {
    let mut inflight: Vec<ReadRequest> = Vec::new();
    let mut batch: Vec<u32> = Vec::new();
    let mut arrays = Vec::new();
    let mut splits: Vec<Vec<ArrayRef>> = Vec::new();
    let mut events = Vec::new();
    loop {
        match scan.step()? {
            Turn::Read(read) => {
                batch.push(*read.segment_id);
                inflight.push(read);
            }
            Turn::Output(split, array) => {
                if !batch.is_empty() {
                    events.push(Event::Io(std::mem::take(&mut batch)));
                }
                assert!(!array.is_empty(), "a scan never emits an empty array");
                events.push(Event::Piece(array.len()));
                if split >= splits.len() {
                    splits.resize_with(split + 1, Vec::new);
                }
                splits[split].push(array.clone());
                arrays.push(array);
            }
            Turn::Child(child) => {
                let (id, child_scan) = child.into_scan(SESSION.clone())?;
                let child = drive_inner(store, child_scan, pick)?;
                assert_eq!(child.leftover, (0, 0));
                events.extend(
                    child
                        .events
                        .into_iter()
                        .filter(|event| !matches!(event, Event::Piece(_))),
                );
                let arrays = child.splits.into_iter().flatten().collect();
                scan.complete_child(id, Some(arrays))?;
            }
            Turn::Waiting => {
                if !batch.is_empty() {
                    events.push(Event::Io(std::mem::take(&mut batch)));
                }
                if inflight.is_empty() {
                    return Err(vortex_err!("scan waits with no reads in flight"));
                }
                let read = inflight.remove(pick(&inflight));
                events.push(Event::Delivered(*read.segment_id));
                scan.deliver(read.id, store.read(read.segment_id))?;
            }
            Turn::Done => break,
        }
    }
    // Reads left in flight were read ahead for splits that emptied or cancelled with the
    // pipeline that asked; nothing waits for them.
    assert_eq!(
        scan.awaited_reads(),
        0,
        "scan finished with a pipeline waiting for a read"
    );
    let leftover = (scan.live_ports(), scan.pending_shares());
    Ok(Run {
        arrays,
        splits,
        events,
        leftover,
    })
}

/// Runs `plan` over one split.
fn run(
    store: &Store,
    plan: &PlanRef,
    rows: Range<u64>,
    mask: Mask,
    pick: impl FnMut(&[ReadRequest]) -> usize,
) -> VortexResult<Run> {
    run_with_values(store, plan, rows, mask, ValueCache::default(), pick)
}

/// [`run`], sharing whole values with the other scans given `values`.
fn run_with_values(
    store: &Store,
    plan: &PlanRef,
    rows: Range<u64>,
    mask: Mask,
    values: ValueCache,
    pick: impl FnMut(&[ReadRequest]) -> usize,
) -> VortexResult<Run> {
    let scan = Scan::try_new(SESSION.clone(), plan.clone(), vec![Split { rows, mask }])?
        .with_value_cache(values);
    let run = drive(store, scan, pick)?;
    assert_eq!(run.leftover, (0, 0), "a finished scan holds no port");
    Ok(run)
}

fn delivery(order: Delivery) -> impl FnMut(&[ReadRequest]) -> usize {
    move |inflight| match order {
        Delivery::Fifo => 0,
        Delivery::Lifo => inflight.len() - 1,
    }
}

#[rstest]
fn grouped_list_ranges_preserve_order_and_shared_reads(
    #[values(Delivery::Fifo, Delivery::Lifo)] order: Delivery,
    #[values(false, true)] shared: bool,
    #[values(false, true)] planned: bool,
) -> VortexResult<()> {
    let mut store = Store::default();
    let elements = Buffer::from_iter(0_i64..20).into_array();
    let offsets = Buffer::from_iter((0_u64..=10).map(|row| row * 2)).into_array();
    let input =
        ListArray::try_new(elements.clone(), offsets.clone(), Validity::NonNullable)?.into_array();
    let layout = if shared {
        store.flat(&elements)?
    } else {
        store.chunked(&elements, &[5, 5, 5, 5])?
    };
    let plan = ListPackPlan::try_new(
        false.into(),
        10,
        lower(&layout)?,
        lower(&store.flat(&offsets)?)?,
        None,
    )?
    .into_plan();
    let plan = if planned { selected_list(plan) } else { plan };
    let rows = [0_u64, 1, 7, 8];
    let mask = Mask::from_iter((0..10).map(|row| rows.contains(&row)));
    let result = run(&store, &plan, 0..10, mask, delivery(order))?;
    let actual = ChunkedArray::try_new(result.arrays, input.dtype().clone())?.into_array();
    let expected = input.take(Buffer::from_iter(rows).into_array())?;
    assert_arrays_eq!(actual, expected, &mut SESSION.create_execution_ctx());
    if get_available_parallelism().unwrap_or(1) > 1 {
        if shared {
            let reads = result
                .events
                .iter()
                .filter(|event| **event == Event::Delivered(0));
            assert_eq!(reads.count(), 1);
        } else {
            assert!(
                result.events.iter().any(|event| {
                    matches!(event, Event::Io(ids) if ids.contains(&0) && ids.contains(&2))
                }),
                "{:?}",
                result.events
            );
        }
    }
    Ok(())
}

fn selected_list(plan: PlanRef) -> PlanRef {
    SyntheticPlan::new(
        plan.dtype().clone(),
        plan.row_count(),
        Vec::new(),
        move |rows, mask, compiler| {
            plan.as_::<ListPack>()
                .compile_selected(rows, mask, compiler, true)
        },
    )
    .into_plan()
}

#[rstest]
fn selected_list_view_skips_unselected_children(
    #[values(Delivery::Fifo, Delivery::Lifo)] order: Delivery,
    #[values(false, true)] empty: bool,
) -> VortexResult<()> {
    let mut store = Store::default();
    let elements = Buffer::from_iter(10_i64..12).into_array();
    let selected = lower(&store.flat(&elements)?)?;
    let unused = SyntheticPlan::new(elements.dtype().clone(), 2, Vec::new(), |_, _, _| {
        Err(vortex_err!("Unselected child must not be compiled"))
    })
    .into_plan();
    let chunks = ConcatPlan::try_new(
        elements.dtype().clone(),
        vec![unused.clone(), selected, unused],
    )?;
    let offsets = Buffer::from(vec![0_u64, 2, 4, 6]).into_array();
    let plan = selected_list(
        ListPackPlan::try_new(
            false.into(),
            3,
            chunks.into_plan(),
            lower(&store.flat(&offsets)?)?,
            None,
        )?
        .into_plan(),
    );
    let result = run(
        &store,
        &plan,
        0..3,
        Mask::from_iter([false, !empty, false]),
        delivery(order),
    )?;
    let expected = ListArray::try_new(
        elements,
        Buffer::from(vec![0_u64, 2]).into_array(),
        Validity::NonNullable,
    )?
    .into_array();
    let expected = if empty {
        expected.slice(0..0)?
    } else {
        expected
    };
    let actual = ChunkedArray::try_new(result.arrays, expected.dtype().clone())?.into_array();
    assert_arrays_eq!(actual, expected, &mut SESSION.create_execution_ctx());
    if !empty {
        assert_eq!(segments_read(&result.events), vec![0, 1]);
    }
    Ok(())
}

#[rstest]
fn dense_nullable_list_ranges_share_one_scan(
    #[values(Delivery::Fifo, Delivery::Lifo)] order: Delivery,
    #[values(false, true)] partial: bool,
    #[values(false, true)] planned: bool,
) -> VortexResult<()> {
    let mut store = Store::default();
    let elements = Buffer::from_iter(0_i64..20).into_array();
    let offsets = Buffer::from_iter((0_u64..=10).map(|row| row * 2)).into_array();
    let valid = BoolArray::from_iter((0..10).map(|row| row != 2 && row != 7)).into_array();
    let input = ListArray::try_new(
        elements.clone(),
        offsets.clone(),
        Validity::Array(valid.clone()),
    )?
    .into_array();
    let plan = ListPackPlan::try_new(
        true.into(),
        10,
        lower(&store.chunked(&elements, &[5, 5, 5, 5])?)?,
        lower(&store.flat(&offsets)?)?,
        Some(lower(&store.flat(&valid)?)?),
    )?
    .into_plan();
    let plan = if planned { selected_list(plan) } else { plan };
    let mask = Mask::from_iter((0..10).map(|row| !partial || row != 4));
    let result = run(&store, &plan, 0..10, mask.clone(), delivery(order))?;
    if !partial {
        assert_eq!(result.arrays[0].as_::<List>().elements().len(), 20);
    }
    let actual = ChunkedArray::try_new(result.arrays, input.dtype().clone())?.into_array();
    assert_arrays_eq!(
        actual,
        input.filter(mask)?,
        &mut SESSION.create_execution_ctx()
    );
    assert!(
        result
            .events
            .iter()
            .any(|event| { matches!(event, Event::Io(ids) if (0..4).all(|id| ids.contains(&id))) }),
        "{:?}",
        result.events
    );
    assert_eq!(segments_read(&result.events), vec![0, 1, 2, 3, 4, 5]);
    Ok(())
}

/// Delivers segments in the given order.
fn scripted(order: &[u32]) -> impl FnMut(&[ReadRequest]) -> usize + '_ {
    let mut next = order.iter();
    move |inflight| {
        let segment = *next.next().expect("script covers every read");
        inflight
            .iter()
            .position(|r| *r.segment_id == segment)
            .expect("scripted segment is in flight")
    }
}

fn reads(events: &[Event]) -> usize {
    events
        .iter()
        .map(|event| match event {
            Event::Io(batch) => batch.len(),
            _ => 0,
        })
        .sum()
}

/// Every segment read, sorted.
fn segments_read(events: &[Event]) -> Vec<u32> {
    let mut read: Vec<u32> = events
        .iter()
        .flat_map(|event| match event {
            Event::Io(batch) => batch.clone(),
            _ => Vec::new(),
        })
        .collect();
    read.sort_unstable();
    read
}

fn pieces(events: &[Event]) -> Vec<usize> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Piece(len) => Some(*len),
            _ => None,
        })
        .collect()
}

/// A view's selection over its rows.
#[derive(Clone, Copy, Debug)]
enum Sel {
    All,
    None,
    EveryOther,
    /// Rows relative to the view start.
    Rows(&'static [usize]),
}

impl Sel {
    fn mask(self, len: usize) -> Mask {
        match self {
            Sel::All => Mask::new_true(len),
            Sel::None => Mask::new_false(len),
            Sel::EveryOther => Mask::from_iter((0..len).map(|i| i % 2 == 0)),
            Sel::Rows(rows) => Mask::from_indices(len, rows.iter().copied()),
        }
    }
}

/// Joins arrays covering consecutive rows into one.
fn join(dtype: &vortex_array::dtype::DType, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
    Ok(ChunkedArray::try_new(arrays, dtype.clone())?.into_array())
}

/// Checks that the arrays, in the order they came out, are the selected rows of the view.
fn assert_view(
    expected: &ArrayRef,
    rows: &Range<u64>,
    mask: &Mask,
    arrays: Vec<ArrayRef>,
) -> VortexResult<()> {
    let actual = join(expected.dtype(), arrays)?;
    let expected = expected
        .slice(rows.start as usize..rows.end as usize)?
        .filter(mask.clone())?;
    let mut ctx = SESSION.create_execution_ctx();
    assert_arrays_eq!(actual, expected, &mut ctx);
    Ok(())
}

#[rstest]
#[case::full(0..20, Sel::All)]
#[case::crosses_every_boundary(3..17, Sel::All)]
#[case::every_other(5..15, Sel::EveryOther)]
#[case::sparse(0..20, Sel::Rows(&[0, 6, 7, 19]))]
#[case::one_row(8..9, Sel::All)]
#[case::inside_one_chunk_per_column(13..16, Sel::All)]
#[case::nothing_selected(2..18, Sel::None)]
#[case::empty_range(5..5, Sel::All)]
fn views_of_one_plan(
    #[case] rows: Range<u64>,
    #[case] sel: Sel,
    #[values(Delivery::Fifo, Delivery::Lifo)] order: Delivery,
) -> VortexResult<()> {
    let mut store = Store::default();
    let (plan, expected) = fixture(&mut store)?;
    let mask = sel.mask((rows.end - rows.start) as usize);

    let run = run(&store, &plan, rows.clone(), mask.clone(), delivery(order))?;
    assert_view(&expected, &rows, &mask, run.arrays)
}

/// Splits cutting every column at different places, run as one scan, each produce their own
/// rows, and a segment several splits read is read and decoded once, then dropped.
#[rstest]
fn splits_of_one_scan_read_each_segment_once(
    #[values([0, 4, 11, 20], [0, 9, 13, 20], [0, 1, 2, 20])] cuts: [u64; 4],
    #[values(1, 3)] active: usize,
    #[values(Delivery::Fifo, Delivery::Lifo)] order: Delivery,
) -> VortexResult<()> {
    let mut store = Store::default();
    let (plan, expected) = fixture(&mut store)?;
    let splits: Vec<Split> = cuts
        .windows(2)
        .map(|w| Split {
            rows: w[0]..w[1],
            mask: Sel::EveryOther.mask((w[1] - w[0]) as usize),
        })
        .collect();
    let scan = Scan::try_new(SESSION.clone(), plan, splits.clone())?.with_max_active(active);
    let run = drive(&store, scan, delivery(order))?;
    assert_eq!(run.leftover, (0, 0), "every shared segment was dropped");
    assert_eq!(
        segments_read(&run.events),
        (0..store.segments.len() as u32).collect::<Vec<_>>()
    );
    for (index, split) in splits.iter().enumerate() {
        assert_view(
            &expected,
            &split.rows,
            &split.mask,
            run.splits.get(index).cloned().unwrap_or_default(),
        )?;
    }
    Ok(())
}

#[rstest]
#[case::gaps(vec![0..2, 8..10, 18..20])]
#[case::overlapping(vec![0..8, 4..12, 10..20])]
#[case::unordered(vec![15..20, 0..4, 7..12])]
#[case::empty(vec![0..0, 4..8, 20..20])]
fn split_ranges_preserve_shared_reads(
    #[case] ranges: Vec<Range<u64>>,
    #[values(1, 3)] active: usize,
    #[values(Delivery::Fifo, Delivery::Lifo)] order: Delivery,
) -> VortexResult<()> {
    let mut store = Store::default();
    let (plan, expected) = fixture(&mut store)?;
    let splits: Vec<_> = ranges.into_iter().map(Split::all).collect();
    let scan = Scan::try_new(SESSION.clone(), plan, splits.clone())?.with_max_active(active);
    let run = drive(&store, scan, delivery(order))?;
    assert_eq!(run.leftover, (0, 0));
    let reads = segments_read(&run.events);
    let mut unique = reads.clone();
    unique.dedup();
    assert_eq!(reads, unique, "each shared segment is read once");
    for (index, split) in splits.iter().enumerate() {
        assert_view(
            &expected,
            &split.rows,
            &split.mask,
            run.splits.get(index).cloned().unwrap_or_default(),
        )?;
    }
    Ok(())
}

#[rstest]
fn chunk_in_gap_between_splits_stays_lazy(#[values(1, 2)] active: usize) -> VortexResult<()> {
    static UNKNOWN_ID: CachedId = CachedId::new("test.unread-gap");
    let mut store = Store::default();
    let values = PrimitiveArray::from_iter(0..ROWS as i32).into_array();
    let known = store.flat(&values)?;
    let unknown = new_foreign_layout(
        *UNKNOWN_ID,
        values.dtype().clone(),
        ROWS,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let layout = ChunkedLayout::new(
        3 * ROWS,
        values.dtype().clone(),
        OwnedLayoutChildren::layout_children(vec![Arc::clone(&known), unknown, known]),
    )
    .into_layout();
    let scan = Scan::try_new(
        SESSION.clone(),
        lower(&layout)?,
        vec![Split::all(0..2), Split::all(2 * ROWS..2 * ROWS + 2)],
    )?
    .with_max_active(active);
    let run = drive(&store, scan, delivery(Delivery::Lifo))?;
    assert_eq!(segments_read(&run.events), vec![0]);
    assert_eq!(run.leftover, (0, 0));
    assert_eq!(run.splits.len(), 2);
    for pieces in run.splits {
        assert_view(&values, &(0..2), &Mask::new_true(2), pieces)?;
    }
    Ok(())
}

#[rstest]
fn chunk_excluded_by_mask_stays_lazy(
    #[values(false, true)] nested: bool,
    #[values(false, true)] empty: bool,
    #[values(false, true)] cached_indices: bool,
) -> VortexResult<()> {
    static UNKNOWN_ID: CachedId = CachedId::new("test.masked-gap");
    let mut store = Store::default();
    let values = PrimitiveArray::from_iter(0..ROWS as i32).into_array();
    let known = store.flat(&values)?;
    let unknown = new_foreign_layout(
        *UNKNOWN_ID,
        values.dtype().clone(),
        ROWS,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let mut layout = ChunkedLayout::new(
        3 * ROWS,
        values.dtype().clone(),
        OwnedLayoutChildren::layout_children(vec![Arc::clone(&known), unknown, Arc::clone(&known)]),
    )
    .into_layout();
    let offset = if nested { ROWS } else { 0 };
    if nested {
        layout = ChunkedLayout::new(
            4 * ROWS,
            values.dtype().clone(),
            OwnedLayoutChildren::layout_children(vec![known, layout]),
        )
        .into_layout();
    }
    let rows = offset + 5..offset + 3 * ROWS - 5;
    let len = usize::try_from(rows.end - rows.start)?;
    let mut mask = if empty {
        Mask::new_false(len)
    } else {
        Mask::from_indices(len, [0, len - 1])
    };
    if !cached_indices {
        mask = Mask::from_buffer(mask.to_bit_buffer());
    }
    let scan = Scan::try_new(SESSION.clone(), lower(&layout)?, vec![Split { rows, mask }])?;
    let run = drive(&store, scan, delivery(Delivery::Lifo))?;
    assert_eq!(run.leftover, (0, 0));
    if empty {
        assert!(segments_read(&run.events).is_empty());
        assert!(run.splits.iter().all(Vec::is_empty));
    } else {
        assert_eq!(segments_read(&run.events), vec![0]);
        let expected = PrimitiveArray::from_iter([5, ROWS as i32 - 6]).into_array();
        assert_view(
            &expected,
            &(0..2),
            &Mask::new_true(2),
            run.splits[0].clone(),
        )?;
    }
    Ok(())
}

#[test]
fn every_read_is_issued_before_any_is_delivered() -> VortexResult<()> {
    let mut store = Store::default();
    let (plan, _) = fixture(&mut store)?;

    let run = run(
        &store,
        &plan,
        0..ROWS,
        Mask::new_true(ROWS as usize),
        delivery(Delivery::Fifo),
    )?;
    let Some(Event::Io(first)) = run.events.first() else {
        return Err(vortex_err!("first event must be a read batch"));
    };
    assert_eq!(first.len(), store.segments.len());
    assert_eq!(reads(&run.events), store.segments.len());
    Ok(())
}

#[test]
fn unselected_chunks_are_never_read() -> VortexResult<()> {
    let mut store = Store::default();
    let (plan, expected) = fixture(&mut store)?;
    // Rows 7..12 only: a reads one chunk, b both, c one, d one, e.x one, e.y one.
    let mask = Mask::from_indices(ROWS as usize, 7..12);

    let run = run(
        &store,
        &plan,
        0..ROWS,
        mask.clone(),
        delivery(Delivery::Fifo),
    )?;
    assert_eq!(reads(&run.events), 7);
    assert_view(&expected, &(0..ROWS), &mask, run.arrays)
}

#[test]
fn nothing_selected_issues_no_io_and_emits_nothing() -> VortexResult<()> {
    let mut store = Store::default();
    let (plan, _) = fixture(&mut store)?;
    let mask = Mask::new_false(ROWS as usize);

    let run = run(&store, &plan, 0..ROWS, mask, delivery(Delivery::Fifo))?;
    assert_eq!(reads(&run.events), 0);
    assert!(run.arrays.is_empty());
    Ok(())
}

/// `{a, b}` with `a` chunked `[0,7) [7,12) [12,20)` and `b` flat.
fn two_columns(store: &mut Store) -> VortexResult<(PlanRef, ArrayRef)> {
    let a = PrimitiveArray::from_iter(0..ROWS as i32).into_array();
    let b = PrimitiveArray::from_iter((0..ROWS as i64).map(|v| v * 10)).into_array();
    let expected = StructArray::from_fields(&[("a", a.clone()), ("b", b.clone())])?.into_array();
    let layout = StructLayout::new(
        ROWS,
        expected.dtype().clone(),
        vec![store.chunked(&a, &[7, 5, 8])?, store.flat(&b)?],
    )
    .into_layout();
    Ok((lower(&layout)?, expected))
}

/// Fields chunked differently are split at their front batch boundaries, in row order,
/// however the reads complete.
///
/// Segments: a0=0, a1=1, a2=2, b=3.
#[rstest]
#[case::b_last(&[1, 0, 2, 3])]
#[case::a_chunk_last(&[3, 0, 2, 1])]
#[case::in_order(&[3, 0, 1, 2])]
fn pack_splits_misaligned_fields(#[case] order: &[u32]) -> VortexResult<()> {
    let mut store = Store::default();
    let (plan, expected) = two_columns(&mut store)?;
    let mask = Mask::new_true(ROWS as usize);

    let run = run(&store, &plan, 0..ROWS, mask.clone(), scripted(order))?;
    assert_eq!(pieces(&run.events), [7, 5, 8]);
    assert_view(&expected, &(0..ROWS), &mask, run.arrays)
}

/// Pack emits as soon as every inlet has a batch, without waiting for an inlet to fill.
#[test]
fn pack_emits_when_every_field_has_data() -> VortexResult<()> {
    let mut store = Store::default();
    let a = PrimitiveArray::from_iter(0..ROWS as i32).into_array();
    let b = PrimitiveArray::from_iter((0..ROWS as i64).map(|v| v * 10)).into_array();
    let expected = StructArray::from_fields(&[("a", a.clone()), ("b", b.clone())])?.into_array();
    let layout = StructLayout::new(
        ROWS,
        expected.dtype().clone(),
        vec![store.chunked(&a, &[1; ROWS as usize])?, store.flat(&b)?],
    )
    .into_layout();
    let plan = lower(&layout)?;
    let mask = Mask::new_true(ROWS as usize);

    let order = std::iter::once(ROWS as u32)
        .chain(0..ROWS as u32)
        .collect::<Vec<_>>();
    let run = run(&store, &plan, 0..ROWS, mask.clone(), scripted(&order))?;
    let delivered_before_output = run
        .events
        .iter()
        .take_while(|event| !matches!(event, Event::Piece(_)))
        .filter_map(|event| match event {
            Event::Delivered(id) => Some(*id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(delivered_before_output, [ROWS as u32, 0]);
    assert_eq!(pieces(&run.events), vec![1; ROWS as usize]);
    assert_view(&expected, &(0..ROWS), &mask, run.arrays)
}

/// Fields chunked alike stream one struct per chunk.
#[test]
fn aligned_fields_stream_one_struct_per_chunk() -> VortexResult<()> {
    let mut store = Store::default();
    let a = PrimitiveArray::from_iter(0..ROWS as i32).into_array();
    let b = PrimitiveArray::from_iter((0..ROWS as i64).map(|v| v * 10)).into_array();
    let expected = StructArray::from_fields(&[("a", a.clone()), ("b", b.clone())])?.into_array();
    let layout = StructLayout::new(
        ROWS,
        expected.dtype().clone(),
        vec![
            store.chunked(&a, &[7, 5, 8])?,
            store.chunked(&b, &[7, 5, 8])?,
        ],
    )
    .into_layout();
    let plan = lower(&layout)?;
    let mask = Mask::new_true(ROWS as usize);

    let run = run(
        &store,
        &plan,
        0..ROWS,
        mask.clone(),
        delivery(Delivery::Fifo),
    )?;
    assert_eq!(pieces(&run.events), [7, 5, 8]);
    assert_view(&expected, &(0..ROWS), &mask, run.arrays)
}

/// A segment scan keeps the rows its split selects, and so does a filter over the same scan,
/// which runs as the same source.
#[rstest]
#[case::every_other(Sel::EveryOther)]
#[case::sparse(Sel::Rows(&[1, 4]))]
#[case::nothing(Sel::None)]
fn scan_keeps_the_selection_with_or_without_a_filter(#[case] sel: Sel) -> VortexResult<()> {
    let mut store = Store::default();
    let values = PrimitiveArray::from_iter(0..ROWS as i32).into_array();
    let filtered = lower(&store.flat(&values)?)?;
    let scan = filtered
        .as_opt::<Filter>()
        .ok_or_else(|| vortex_err!("A flat layout lowers to a filter"))?
        .child_plan()?;
    assert!(scan.is::<SegmentScan>());

    let rows = 3..9;
    let mask = sel.mask(6);
    for plan in [&scan, &filtered] {
        let kept = run(
            &store,
            plan,
            rows.clone(),
            mask.clone(),
            delivery(Delivery::Fifo),
        )?;
        if mask.all_false() {
            assert_eq!(reads(&kept.events), 0, "nothing selected reads nothing");
        }
        assert_view(&values, &rows, &mask, kept.arrays)?;
    }
    Ok(())
}

/// A take reads its values over their whole domain and its codes over the selection, including
/// when a predicate has been pushed onto the values, and emits nothing before the values are
/// whole. Later scans sharing its value cache reuse the values and read only the codes; a scan
/// with a cache of its own reads them again.
#[rstest]
#[case::values(false)]
#[case::predicate(true)]
fn take_waits_for_whole_values_and_keeps_them(#[case] predicate: bool) -> VortexResult<()> {
    let mut store = Store::default();
    let values = VarBinViewArray::from_iter_str(["a", "b", "c"]).into_array();
    let codes = PrimitiveArray::from_iter((0..ROWS).map(|v| (v % 3) as u8)).into_array();
    let layout = DictLayout::new(store.flat(&values)?, store.flat(&codes)?).into_layout();
    let mut plan = lower(&layout)?;
    let expected = if predicate {
        BoolArray::from_iter((0..ROWS).map(|row| row % 3 != 0)).into_array()
    } else {
        VarBinViewArray::from_iter_str((0..ROWS).map(|row| ["a", "b", "c"][(row % 3) as usize]))
            .into_array()
    };
    if predicate {
        let expression = gt(root(), lit("a"))
            .bind(plan.dtype())?
            .optimize_recursive()?;
        plan = optimize(EvalPlan::try_new(expression, plan)?.into_plan())?;
    }
    assert!(plan.is::<Take>());

    let mask = Sel::EveryOther.mask(10);
    let cache = ValueCache::default();
    // Codes (segment 1) land first; nothing comes out until the values (segment 0) do.
    let first = run_with_values(
        &store,
        &plan,
        0..10,
        mask.clone(),
        cache.clone(),
        scripted(&[1, 0]),
    )?;
    assert_eq!(reads(&first.events), 2);
    let first_piece = first
        .events
        .iter()
        .position(|e| matches!(e, Event::Piece(_)))
        .vortex_expect("a piece");
    assert!(
        first.events[first_piece..]
            .iter()
            .all(|e| !matches!(e, Event::Delivered(_))),
        "the only array must follow both deliveries"
    );
    assert_view(&expected, &(0..10), &mask, first.arrays)?;

    let rows = 10..20;
    let shared = run_with_values(
        &store,
        &plan,
        rows.clone(),
        mask.clone(),
        cache,
        delivery(Delivery::Fifo),
    )?;
    assert_eq!(reads(&shared.events), 1, "only the codes are read again");
    assert_view(&expected, &rows, &mask, shared.arrays)?;

    let fresh = run(
        &store,
        &plan,
        rows.clone(),
        mask.clone(),
        delivery(Delivery::Fifo),
    )?;
    assert_eq!(
        reads(&fresh.events),
        2,
        "a new cache reads the values again"
    );
    assert_view(&expected, &rows, &mask, fresh.arrays)
}

#[test]
fn eval_applies_expression_to_selected_rows() -> VortexResult<()> {
    let mut store = Store::default();
    let (plan, _) = two_columns(&mut store)?;
    let expression = gt(get_item("a", root()), lit(4_i32)).bind(plan.dtype())?;
    let expected = BoolArray::from_iter((0..ROWS).map(|row| row > 4)).into_array();
    let plan = EvalPlan::try_new(expression, plan)?.into_plan();

    let rows = 2..18;
    let mask = Sel::EveryOther.mask(16);
    for plan in [plan.clone(), optimize(plan)?] {
        let run = run(
            &store,
            &plan,
            rows.clone(),
            mask.clone(),
            delivery(Delivery::Lifo),
        )?;
        assert_view(&expected, &rows, &mask, run.arrays)?;
    }
    Ok(())
}

/// A column read by two fields of one struct is read and decoded once, and both fields get it.
#[test]
fn a_segment_two_readers_need_is_read_once() -> VortexResult<()> {
    let mut store = Store::default();
    let a = PrimitiveArray::from_iter(0..ROWS as i32).into_array();
    let column = lower(&store.chunked(&a, &[7, 13])?)?;
    let expected = StructArray::from_fields(&[("x", a.clone()), ("y", a)])?.into_array();
    let fields = expected
        .dtype()
        .as_struct_fields_opt()
        .vortex_expect("struct")
        .clone();
    let plan = PackPlan::try_new(
        fields,
        vortex_array::dtype::Nullability::NonNullable,
        ROWS,
        vec![column.clone(), column],
        None,
    )?
    .into_plan();
    let rows = 3..17;
    let mask = Sel::EveryOther.mask(14);

    let run = run(
        &store,
        &plan,
        rows.clone(),
        mask.clone(),
        delivery(Delivery::Lifo),
    )?;
    assert_eq!(segments_read(&run.events), [0, 1]);
    assert_view(&expected, &rows, &mask, run.arrays)
}

#[rstest]
fn pack_readiness_tracks_changes_and_consumption(
    #[values(2, 3)] second_len: u64,
) -> VortexResult<()> {
    let mut arena = Arena::default();
    let inlets = [
        arena.create(8, None, SourceEnd::Unclaimed),
        arena.create(8, None, SourceEnd::Unclaimed),
    ];
    let mut bytes = VecDeque::new();
    let mut reads = Default::default();
    let mut exec = SESSION.create_execution_ctx();
    let mut touched = InletSet::new(2);
    let mut changed = InletSet::all(2);
    let mut child_requests = Vec::new();
    let mut child_inbox = None;
    let mut cx = Cx {
        child_requests: &mut child_requests,
        child_inbox: &mut child_inbox,
        arena: &mut arena,
        inlets: &inlets,
        bytes: &mut bytes,
        reads: &mut reads,
        session: &SESSION,
        exec: &mut exec,
        touched: &mut touched,
        changed: &mut changed,
    };
    let fields = StructFields::from_iter([("a", row_dtype()), ("b", row_dtype())]);
    let mut pack = PackSource::new(fields, false, 2);
    assert!(matches!(
        pack.ready(&mut cx)?,
        SourceReady::Blocked(SourceBlocked::Inlet(_))
    ));

    cx.arena
        .push(inlets[0], Buffer::from_iter(0_u64..2).into_array());
    cx.arena.close(inlets[0]);
    cx.changed.insert(0);
    assert_eq!(
        pack.ready(&mut cx)?,
        SourceReady::Blocked(SourceBlocked::Inlet(1))
    );
    cx.touched.clear();
    assert_eq!(
        pack.ready(&mut cx)?,
        SourceReady::Blocked(SourceBlocked::Inlet(1))
    );
    assert!(
        cx.touched.indices().is_empty(),
        "unchanged inlets were scanned"
    );

    cx.arena
        .push(inlets[1], Buffer::from_iter(0..second_len).into_array());
    cx.arena.close(inlets[1]);
    cx.changed.insert(1);
    assert_eq!(pack.ready(&mut cx)?, SourceReady::Ready);
    assert_eq!(cx.touched.indices(), &[1]);
    let Step::Final(array) = pack.compute(Input::None, &mut cx)? else {
        return Err(vortex_err!(
            "expected Pack's last currently available batch"
        ));
    };
    let expected = StructArray::from_fields(&[
        ("a", Buffer::from_iter(0_u64..2).into_array()),
        ("b", Buffer::from_iter(0_u64..2).into_array()),
    ])?;
    assert_arrays_eq!(array, expected.into_array(), cx.exec());

    // Computing consumed every available row from the first inlet, without writer changes.
    cx.touched.clear();
    let ready = pack.ready(&mut cx);
    assert!(
        cx.touched.indices().is_empty(),
        "consumed inlets were rescanned"
    );
    if second_len == 2 {
        assert_eq!(ready?, SourceReady::Ready);
        assert!(matches!(pack.compute(Input::None, &mut cx)?, Step::End));
    } else {
        let error = ready.expect_err("a remaining row must fail Pack readiness");
        assert!(
            error
                .to_string()
                .contains("Pack fields ended at different rows")
        );
    }
    Ok(())
}

#[rstest]
fn nullable_struct_without_fields_keeps_its_rows(
    #[values(Sel::All, Sel::EveryOther)] selection: Sel,
) -> VortexResult<()> {
    let mut store = Store::default();
    let validity = BoolArray::from_iter((0..ROWS).map(|row| row % 3 != 0)).into_array();
    let expected = StructArray::try_new_with_dtype(
        Vec::new(),
        StructFields::empty(),
        ROWS as usize,
        Validity::Array(validity.clone()),
    )?
    .into_array();
    let layout = StructLayout::new(ROWS, expected.dtype().clone(), vec![store.flat(&validity)?])
        .into_layout();
    let mask = selection.mask(ROWS as usize);
    let run = run(
        &store,
        &lower(&layout)?,
        0..ROWS,
        mask.clone(),
        delivery(Delivery::Fifo),
    )?;
    assert_view(&expected, &(0..ROWS), &mask, run.arrays)
}
