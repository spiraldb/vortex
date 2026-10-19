// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Isolates scan preparation, compilation, and segment delivery from Arrow conversion and IO.

#![expect(clippy::unwrap_used)]

use std::hint::black_box;

use divan::Bencher;
use mimalloc::MiMalloc;
use vortex_array::ArrayContext;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::StructFields;
use vortex_array::serde::SerializeOptions;
use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_buffer::ByteBufferMut;
use vortex_layout::plan::physical::ConcatPlan;
use vortex_layout::plan::physical::FilterPlan;
use vortex_layout::plan::physical::ListPackPlan;
use vortex_layout::plan::physical::PackPlan;
use vortex_layout::plan::physical::PlanRef;
use vortex_layout::plan::physical::SegmentScanPlan;
use vortex_layout::plan::physical::pipeline::Scan;
use vortex_layout::plan::physical::pipeline::Split;
use vortex_layout::plan::physical::pipeline::Turn;
use vortex_layout::segments::SegmentId;
use vortex_layout::session::LayoutSession;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const ROWS: u64 = 1024;

fn main() {
    divan::main();
}

struct Fixture {
    session: VortexSession,
    plan: PlanRef,
    segment: BufferHandle,
}

impl Fixture {
    fn new(columns: u32) -> Self {
        let session = vortex_array::array_session().with::<LayoutSession>();
        let array = PrimitiveArray::from_iter(0_i32..1024).into_array();
        let context = ArrayContext::empty();
        let buffers = array
            .serialize(
                &context,
                &session,
                &SerializeOptions {
                    offset: 0,
                    include_padding: true,
                },
            )
            .unwrap();
        let mut bytes = ByteBufferMut::empty_aligned(Alignment::new(64));
        for buffer in buffers {
            bytes.extend_from_slice(buffer.as_ref());
        }
        let fields = StructFields::from_iter(
            (0..columns).map(|index| (format!("c{index}"), array.dtype().clone())),
        );
        let children = (0..columns)
            .map(|index| {
                SegmentScanPlan::new(
                    array.dtype().clone(),
                    ROWS,
                    SegmentId::from(index),
                    ReadContext::new(context.to_ids()),
                    None,
                )
                .into_plan()
            })
            .collect();
        Self {
            session,
            plan: PackPlan::try_new(fields, Nullability::NonNullable, ROWS, children, None)
                .unwrap()
                .into_plan(),
            segment: BufferHandle::new_host(bytes.freeze()),
        }
    }

    fn scan(&self) -> Scan {
        Scan::try_new(
            self.session.clone(),
            self.plan.clone(),
            vec![Split::all(0..ROWS)],
        )
        .unwrap()
    }
}

#[divan::bench(args = [1, 100, 1000])]
fn prepare(bencher: Bencher, columns: u32) {
    let fixture = Fixture::new(columns);
    bencher.bench_local(|| black_box(fixture.scan()));
}

#[divan::bench(args = [1, 100, 1000])]
fn prepare_and_compile(bencher: Bencher, columns: u32) {
    let fixture = Fixture::new(columns);
    bencher.bench_local(|| {
        let mut scan = fixture.scan();
        assert!(matches!(scan.step().unwrap(), Turn::Read(_)));
        black_box(scan);
    });
}

#[divan::bench(args = [1, 100, 1000])]
fn scan(bencher: Bencher, columns: u32) {
    let fixture = Fixture::new(columns);
    bencher.bench_local(|| {
        let mut scan = fixture.scan();
        loop {
            match scan.step().unwrap() {
                Turn::Read(read) => scan.deliver(read.id, fixture.segment.clone()).unwrap(),
                Turn::Output(_, array) => {
                    black_box(array);
                }
                Turn::Child(_) => unreachable!("benchmark has no asynchronous child work"),
                Turn::Waiting => unreachable!("every read is delivered immediately"),
                Turn::Done => break,
            }
        }
    });
}

struct ListFixture {
    session: VortexSession,
    plan: PlanRef,
    segments: Vec<BufferHandle>,
    mask: Mask,
}

impl ListFixture {
    fn new(chunks: u32, uniform: bool) -> Self {
        const LISTS: usize = 65_536;
        const ELEMENTS_PER_LIST: usize = 8;
        let session = vortex_array::array_session().with::<LayoutSession>();
        let chunk_len = LISTS * ELEMENTS_PER_LIST / chunks as usize;
        let elements = Buffer::from_iter(0..chunk_len as u64).into_array();
        let offsets = Buffer::from_iter((0..=LISTS as u64).map(|row| row * 8)).into_array();
        let serialize = |array: &ArrayRef| {
            let context = ArrayContext::empty();
            let buffers = array
                .serialize(
                    &context,
                    &session,
                    &SerializeOptions {
                        offset: 0,
                        include_padding: true,
                    },
                )
                .unwrap();
            let mut bytes = ByteBufferMut::empty_aligned(Alignment::new(64));
            for buffer in buffers {
                bytes.extend_from_slice(buffer.as_ref());
            }
            (BufferHandle::new_host(bytes.freeze()), context)
        };
        let (element_bytes, element_context) = serialize(&elements);
        let (offset_bytes, offset_context) = serialize(&offsets);
        let children = (0..chunks)
            .map(|id| {
                FilterPlan::new(
                    SegmentScanPlan::new(
                        elements.dtype().clone(),
                        chunk_len as u64,
                        SegmentId::from(id),
                        ReadContext::new(element_context.to_ids()),
                        None,
                    )
                    .into_plan(),
                )
                .into_plan()
            })
            .collect();
        let elements = ConcatPlan::try_new(elements.dtype().clone(), children).unwrap();
        let offsets = FilterPlan::new(
            SegmentScanPlan::new(
                offsets.dtype().clone(),
                LISTS as u64 + 1,
                SegmentId::from(chunks),
                ReadContext::new(offset_context.to_ids()),
                None,
            )
            .into_plan(),
        );
        let plan = ListPackPlan::try_new(
            Nullability::NonNullable,
            LISTS as u64,
            elements.into_plan(),
            offsets.into_plan(),
            None,
        )
        .unwrap()
        .into_plan();
        let mut segments = vec![element_bytes; chunks as usize];
        segments.push(offset_bytes);
        let indices = (0..100).map(|index| if uniform { index * (LISTS / 100) } else { index });
        Self {
            session,
            plan,
            segments,
            mask: Mask::from_indices(LISTS, indices),
        }
    }

    fn scan(&self) {
        let split = Split {
            rows: 0..self.mask.len() as u64,
            mask: self.mask.clone(),
        };
        let mut scan = Scan::try_new(self.session.clone(), self.plan.clone(), vec![split]).unwrap();
        loop {
            match scan.step().unwrap() {
                Turn::Read(read) => {
                    scan.deliver(read.id, self.segments[*read.segment_id as usize].clone())
                        .unwrap();
                }
                Turn::Output(_, array) => {
                    assert_eq!(array.len(), 100);
                    black_box(array);
                }
                Turn::Child(_) => unreachable!("microbenchmark has no child planner"),
                Turn::Waiting => unreachable!("every read is delivered immediately"),
                Turn::Done => break,
            }
        }
    }
}

/// Fixed selected row count, varying physical chunk count. Compare in separate processes with
/// `VORTEX_LIST_PIPELINE` unset and set to `1`; no disk IO or Arrow conversion is timed.
#[divan::bench(args = [8, 128, 1024], consts = [false, true])]
fn list_scan<const UNIFORM: bool>(bencher: Bencher, chunks: u32) {
    let fixture = ListFixture::new(chunks, UNIFORM);
    bencher.bench_local(|| fixture.scan());
}
