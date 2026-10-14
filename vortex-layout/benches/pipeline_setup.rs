// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! A small split near the end of a large file must not pay for earlier segment IDs.

use divan::Bencher;
use mimalloc::MiMalloc;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability::NonNullable;
use vortex_array::dtype::PType;
use vortex_error::VortexExpect;
use vortex_layout::plan::physical::SegmentScanPlan;
use vortex_layout::plan::physical::pipeline::Scan;
use vortex_layout::plan::physical::pipeline::Split;
use vortex_layout::segments::SegmentId;
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
