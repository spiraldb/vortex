// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Scan setup must scale with the segments and chunk boundaries it uses.

use std::sync::Arc;

use divan::Bencher;
use mimalloc::MiMalloc;
use vortex_array::ArrayRef;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability::NonNullable;
use vortex_array::dtype::PType;
use vortex_array::expr::gt;
use vortex_array::expr::lit;
use vortex_array::expr::root;
use vortex_error::VortexExpect;
use vortex_error::vortex_panic;
use vortex_layout::LayoutReaderRef;
use vortex_layout::layout_children;
use vortex_layout::layouts::chunked::ChunkedLayout;
use vortex_layout::layouts::flat::FlatLayout;
use vortex_layout::plan::physical::ConcatPlan;
use vortex_layout::plan::physical::FilterPlan;
use vortex_layout::plan::physical::SegmentScanPlan;
use vortex_layout::plan::physical::optimize;
use vortex_layout::plan::physical::pipeline::Scan;
use vortex_layout::plan::physical::pipeline::Split;
use vortex_layout::scan::v2::ScanBuilder;
use vortex_layout::scan::v2::ScanFile;
use vortex_layout::segments::SegmentFuture;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    divan::main();
}

#[divan::bench(args = [0, 1024, 65536])]
fn single_segment(bencher: Bencher, segment_id: u32) {
    let session = vortex_array::array_session();
    let plan = SegmentScanPlan::new(
        DType::Primitive(PType::I32, NonNullable),
        1024,
        SegmentId::from(segment_id),
        ReadContext::new([]),
        None,
    )
    .into_plan();

    bencher.bench_local(|| {
        Scan::try_new(session.clone(), plan.clone(), vec![Split::all(0..1024)])
            .vortex_expect("valid scan")
    });
}

#[divan::bench(args = [16, 1024, 16384])]
fn unchanged_plan(bencher: Bencher, chunks: u32) {
    let dtype = DType::Primitive(PType::I32, NonNullable);
    let children = (0..chunks)
        .map(|id| {
            FilterPlan::new(
                SegmentScanPlan::new(dtype.clone(), 1024, id.into(), ReadContext::new([]), None)
                    .into_plan(),
            )
            .into_plan()
        })
        .collect();
    let plan = ConcatPlan::try_new(dtype, children)
        .vortex_expect("valid concatenation")
        .into_plan();
    bencher.bench_local(|| optimize(plan.clone()).vortex_expect("valid plan"));
}

struct NoSegments;

impl SegmentSource for NoSegments {
    fn request(&self, _id: SegmentId) -> SegmentFuture {
        vortex_panic!("scan preparation must not read segments")
    }
}

#[divan::bench(args = [16, 1024, 16384])]
fn chunk_boundaries(bencher: Bencher, chunks: u32) {
    let (session, reader, file) = chunked_file(chunks);
    bencher.bench_local(|| {
        ScanBuilder::<ArrayRef>::new(session.clone(), Arc::clone(&reader), file.clone())
            .prepare()
            .vortex_expect("valid scan")
    });
}

#[divan::bench(args = [16, 1024, 16384])]
fn partitioned_filter(bencher: Bencher, chunks: u32) {
    let (session, reader, file) = chunked_file(chunks);
    let filter = gt(root(), lit(5_i32))
        .bind(file.layout.dtype())
        .vortex_expect("valid predicate");
    bencher.bench_local(|| {
        (0..8)
            .map(|_| {
                ScanBuilder::<ArrayRef>::new(session.clone(), Arc::clone(&reader), file.clone())
                    .with_filter(filter.clone())
                    .prepare()
                    .vortex_expect("valid scan")
            })
            .collect::<Vec<_>>()
    });
}

fn chunked_file(chunks: u32) -> (VortexSession, LayoutReaderRef, ScanFile) {
    let session = vortex_array::array_session().with::<LayoutSession>();
    let dtype = DType::Primitive(PType::I32, NonNullable);
    // Each chunk exceeds the split cap, so every coarse split attempts refinement.
    let chunk_rows = 1 << 17;
    let children = (0..chunks)
        .map(|id| {
            FlatLayout::new(chunk_rows, dtype.clone(), id.into(), ReadContext::new([]))
                .into_layout()
        })
        .collect();
    let layout = ChunkedLayout::new(
        u64::from(chunks) * chunk_rows,
        dtype,
        layout_children(children),
    )
    .into_layout();
    let segments: Arc<dyn SegmentSource> = Arc::new(NoSegments);
    let reader = layout
        .new_reader(
            "bench".into(),
            Arc::clone(&segments),
            &session,
            &Default::default(),
        )
        .vortex_expect("valid reader");
    let file = ScanFile {
        layout,
        locations: Arc::default(),
        segments,
        io: None,
        plans: None,
    };

    (session, reader, file)
}
