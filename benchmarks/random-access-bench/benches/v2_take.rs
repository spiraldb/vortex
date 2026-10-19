// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Each random-access query with the default and V2 scans, over files in the page cache.
//!
//! Run `random-access-bench --prepare-data --formats vortex` first to write the files.

#![expect(clippy::unwrap_used)]

use std::hint::black_box;
use std::sync::LazyLock;

use divan::Bencher;
use mimalloc::MiMalloc;
use random_access_bench::AccessPattern;
use random_access_bench::generate_indices;
use tokio::runtime::Runtime;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::array::stream::ArrayStreamExt;
use vortex::buffer::Buffer;
use vortex::file::OpenOptionsSessionExt;
use vortex::file::VortexFile;
use vortex::layout::scan::v2::ScanBuilder;
use vortex::scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_bench::Format;
use vortex_bench::SESSION;
use vortex_bench::datasets::feature_vectors::FeatureVectorsData;
use vortex_bench::datasets::nested_structs::NestedStructsData;
use vortex_bench::datasets::taxi_data::TaxiData;
use vortex_bench::random_access::BenchDataset;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());

/// The queries V2 can scan: nested lists use list layouts, which V2 does not support.
const QUERIES: &[&str] = &[
    "taxi/correlated",
    "taxi/uniform",
    "feature-vectors/correlated",
    "feature-vectors/uniform",
    "nested-structs/correlated",
    "nested-structs/uniform",
];

fn main() {
    divan::main();
}

struct Query {
    file: VortexFile,
    indices: Vec<u64>,
}

impl Query {
    fn open(query: &str) -> Self {
        let (dataset, pattern) = query.split_once('/').unwrap();
        let dataset: Box<dyn BenchDataset> = match dataset {
            "taxi" => Box::new(TaxiData),
            "feature-vectors" => Box::new(FeatureVectorsData),
            _ => Box::new(NestedStructsData),
        };
        let pattern = match pattern {
            "correlated" => AccessPattern::Correlated,
            _ => AccessPattern::Uniform,
        };
        let indices = generate_indices(dataset.as_ref(), pattern);
        let file = RUNTIME.block_on(async {
            let path = dataset.path(Format::OnDiskVortex).await.unwrap();
            SESSION
                .open_options()
                .with_layout_reader_cache()
                .open_path(path)
                .await
                .unwrap()
        });
        Self { file, indices }
    }

    fn indices(&self) -> StrictSortedBuffer<u64> {
        StrictSortedBuffer::try_new(Buffer::from(self.indices.clone())).unwrap()
    }

    fn take_v1(&self) {
        let array = RUNTIME.block_on(
            self.file
                .scan()
                .unwrap()
                .with_row_indices(self.indices())
                .into_array_stream()
                .unwrap()
                .read_all(),
        );
        black_box(array.unwrap());
    }

    fn scan_v2(&self, builder: impl FnOnce(ScanBuilder) -> ScanBuilder) {
        let stream = builder(ScanBuilder::new(SESSION.clone(), self.file.scan_file()))
            .into_stream()
            .unwrap();
        let array =
            RUNTIME.block_on(ArrayStreamAdapter::new(self.file.dtype().clone(), stream).read_all());
        black_box(array.unwrap());
    }
}

/// The default scan, the target V2 should beat.
#[divan::bench(args = QUERIES)]
fn take_v1(bencher: Bencher, query: &str) {
    let query = Query::open(query);
    bencher.bench_local(|| query.take_v1());
}

#[divan::bench(args = QUERIES)]
fn take_v2(bencher: Bencher, query: &str) {
    let query = Query::open(query);
    bencher.bench_local(|| query.scan_v2(|scan| scan.with_row_indices(query.indices())));
}

/// A V2 scan that selects no rows: its per-scan cost before any split is planned.
#[divan::bench(args = QUERIES)]
fn setup_v2(bencher: Bencher, query: &str) {
    let query = Query::open(query);
    bencher.bench_local(|| query.scan_v2(|scan| scan.with_row_range(0..0)));
}
