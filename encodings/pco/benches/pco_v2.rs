// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Pco v1 at several page sizes against Pco v2 (one 262k-value page per chunk with checkpoints
//! every 1,024 values) on a sorted and a random column of 2^20 values. Cases are
//! `dataset/encoding`.

#![expect(clippy::cast_possible_truncation)]
#![expect(clippy::cast_precision_loss)]

use std::sync::LazyLock;

use divan::Bencher;
use mimalloc::MiMalloc;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_error::VortexExpect;
use vortex_mask::Mask;
use vortex_pco::Pco;
use vortex_pco::PcoOptions;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    divan::main();
}

const LEN: usize = 1 << 20;

const CASES: &[&str] = &[
    "timestamps/pco-262k",
    "timestamps/pco-8k",
    "timestamps/pco-1k",
    "timestamps/pco2",
    "prices/pco-262k",
    "prices/pco-8k",
    "prices/pco-1k",
    "prices/pco2",
];

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    vortex_pco::initialize(&session);
    session
});

fn lcg(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *seed >> 33
}

struct Dataset {
    name: &'static str,
    array: PrimitiveArray,
    /// The 1st percentile, so `< p1` selects 1% of rows.
    p1: Scalar,
}

fn dataset(case: &str) -> &'static Dataset {
    let (name, _) = case
        .split_once('/')
        .vortex_expect("case is dataset/encoding");
    DATASETS
        .iter()
        .find(|dataset| dataset.name == name)
        .vortex_expect("known dataset")
}

fn encoded(case: &str) -> &'static ArrayRef {
    &ENCODED
        .iter()
        .find(|(name, _)| *name == case)
        .vortex_expect("known case")
        .1
}

static DATASETS: LazyLock<Vec<Dataset>> = LazyLock::new(|| {
    let mut seed = 7;
    let mut t = 1_700_000_000_000_000i64;
    let timestamps = (0..LEN)
        .map(|_| {
            t += 1_000_000 + (lcg(&mut seed) % 1000) as i64;
            t
        })
        .collect::<Vec<_>>();
    let prices = (0..LEN)
        .map(|_| (lcg(&mut seed) % 1_000_000) as f64 / 100.0)
        .collect::<Vec<_>>();
    let mut sorted_prices = prices.clone();
    sorted_prices.sort_by(f64::total_cmp);
    vec![
        Dataset {
            name: "timestamps",
            p1: Scalar::from(timestamps[LEN / 100]),
            array: PrimitiveArray::from_iter(timestamps),
        },
        Dataset {
            name: "prices",
            p1: Scalar::from(sorted_prices[LEN / 100]),
            array: PrimitiveArray::from_iter(prices),
        },
    ]
});

static ENCODED: LazyLock<Vec<(&'static str, ArrayRef)>> = LazyLock::new(|| {
    let mut ctx = SESSION.create_execution_ctx();
    CASES
        .iter()
        .map(|&case| {
            let (_, encoding) = case
                .split_once('/')
                .vortex_expect("case is dataset/encoding");
            let array = dataset(case).array.as_view();
            let encoded = match encoding {
                "pco-262k" => Pco::from_primitive(array, 8, 0, &mut ctx).map(IntoArray::into_array),
                "pco-8k" => {
                    Pco::from_primitive(array, 8, 8192, &mut ctx).map(IntoArray::into_array)
                }
                "pco-1k" => {
                    Pco::from_primitive(array, 8, 1024, &mut ctx).map(IntoArray::into_array)
                }
                "pco2" => {
                    Pco::compress(array, &PcoOptions::v2(), &mut ctx).map(IntoArray::into_array)
                }
                _ => unreachable!("unknown encoding {encoding}"),
            };
            (case, encoded.vortex_expect("compression"))
        })
        .collect()
});

fn indices(count: usize) -> Vec<usize> {
    let mut seed = 42;
    (0..count).map(|_| lcg(&mut seed) as usize % LEN).collect()
}

#[divan::bench(args = CASES)]
fn decompress(bencher: Bencher, case: &str) {
    let array = encoded(case);
    bencher
        .counter(divan::counter::ItemsCount::new(LEN))
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            array
                .clone()
                .execute::<PrimitiveArray>(ctx)
                .vortex_expect("decompress")
        });
}

#[divan::bench(args = CASES)]
fn scalar_at(bencher: Bencher, case: &str) {
    let array = encoded(case);
    let indices = indices(16);
    bencher
        .counter(divan::counter::ItemsCount::new(indices.len()))
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            for &index in &indices {
                divan::black_box(array.execute_scalar(index, ctx).vortex_expect("scalar_at"));
            }
        });
}

#[divan::bench(args = CASES)]
fn take_100(bencher: Bencher, case: &str) {
    let array = encoded(case);
    let indices =
        PrimitiveArray::from_iter(indices(100).into_iter().map(|i| i as u64)).into_array();
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            array
                .take(indices.clone())
                .vortex_expect("take")
                .execute::<PrimitiveArray>(ctx)
                .vortex_expect("take")
        });
}

#[divan::bench(args = CASES)]
fn filter_clustered_0_1pct(bencher: Bencher, case: &str) {
    let array = encoded(case);
    let start = LEN / 3;
    let mask = Mask::from_slices(LEN, vec![(start, start + LEN / 1000)]);
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            array
                .filter(mask.clone())
                .vortex_expect("filter")
                .execute::<PrimitiveArray>(ctx)
                .vortex_expect("filter")
        });
}

#[divan::bench(args = CASES)]
fn compare_lt_p1(bencher: Bencher, case: &str) {
    let array = encoded(case);
    let rhs = ConstantArray::new(dataset(case).p1.clone(), LEN).into_array();
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            array
                .binary(rhs.clone(), Operator::Lt)
                .vortex_expect("compare")
                .execute::<BoolArray>(ctx)
                .vortex_expect("compare")
        });
}
