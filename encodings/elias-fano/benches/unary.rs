// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Baselines for the naive `Unary` encoding, so each later optimisation can be measured
//! against them. Keep the names and inputs stable: CodSpeed compares benchmarks by name.
//!
//! Four shapes, all small enough for the encoder's size rule:
//!
//! * `Dense`: values drawn from `[0, LEN)`, about one unset bit per row.
//! * `Spread`: values drawn from `[0, 2 * LEN)`, the densest an Elias-Fano high part gets.
//! * `Runs`: `LEN / 64` values, 64 rows each, so set bits come in long runs.
//! * `Gap`: dense halves either side of one jump of `GAP` values, a long run of unset bits that
//!   every scan crossing it has to walk.

#![expect(clippy::unwrap_used)]

use std::sync::LazyLock;

use divan::Bencher;
use mimalloc::MiMalloc;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::Primitive;
use vortex_array::arrays::PrimitiveArray;
use vortex_elias_fano::unary_encode;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    vortex_elias_fano::initialize(&session);
    session
});

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Dense,
    Spread,
    Runs,
    Gap,
}

const SHAPES: &[Shape] = &[Shape::Dense, Shape::Spread, Shape::Runs, Shape::Gap];

/// Rows per array, small enough that a whole decode stays inside the 1 ms per-iteration budget.
const LEN: usize = 1 << 16;

/// The jump in `Gap`, in values.
const GAP: u64 = 4 * LEN as u64;

/// Rows per value in `Runs`.
const ROWS_PER_VALUE: usize = 64;

/// Probes per iteration. Each is a linear scan from the first bit, so 256 keep an iteration near
/// the budget, as in `vortex-array`'s probe benchmark.
const PROBES: usize = 256;

fn values(shape: Shape) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(0);
    let len = LEN as u64;
    let mut out: Vec<u64> = match shape {
        Shape::Dense => (0..LEN).map(|_| rng.random_range(0..len)).collect(),
        Shape::Spread => (0..LEN).map(|_| rng.random_range(0..2 * len)).collect(),
        Shape::Runs => (0..LEN).map(|row| (row / ROWS_PER_VALUE) as u64).collect(),
        Shape::Gap => (0..LEN)
            .map(|row| {
                let offset = if row < LEN / 2 { 0 } else { GAP };
                offset + rng.random_range(0..len / 2)
            })
            .collect(),
    };
    out.sort_unstable();
    out
}

fn primitive(shape: Shape) -> PrimitiveArray {
    PrimitiveArray::from_iter(values(shape))
}

fn encoded(shape: Shape) -> ArrayRef {
    unary_encode(primitive(shape).as_ref().as_::<Primitive>())
        .unwrap()
        .unwrap()
        .into_array()
}

#[divan::bench(args = SHAPES)]
fn encode(bencher: Bencher, shape: Shape) {
    let array = primitive(shape);
    bencher.bench_local(|| {
        divan::black_box(unary_encode(array.as_ref().as_::<Primitive>()).unwrap());
    });
}

/// The whole array, in one walk over the set bits.
#[divan::bench(args = SHAPES)]
fn decode(bencher: Bencher, shape: Shape) {
    let array = encoded(shape);
    bencher
        .with_inputs(|| (array.clone(), SESSION.create_execution_ctx()))
        .bench_local_values(|(array, mut ctx)| {
            divan::black_box(array.execute::<PrimitiveArray>(&mut ctx).unwrap());
        });
}

/// Point reads in a random order, each a scan from the first bit.
#[divan::bench(args = SHAPES)]
fn scalar_at_random(bencher: Bencher, shape: Shape) {
    let array = encoded(shape);
    let mut rng = StdRng::seed_from_u64(1);
    let indices: Vec<usize> = (0..PROBES).map(|_| rng.random_range(0..LEN)).collect();
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_local_values(|mut ctx| {
            for &index in &indices {
                divan::black_box(array.execute_scalar(index, &mut ctx).unwrap());
            }
        });
}

/// Consecutive point reads from the middle, which a cursor could answer from the last position.
#[divan::bench(args = SHAPES)]
fn scalar_at_sequential(bencher: Bencher, shape: Shape) {
    let array = encoded(shape);
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_local_values(|mut ctx| {
            for index in LEN / 2..LEN / 2 + PROBES {
                divan::black_box(array.execute_scalar(index, &mut ctx).unwrap());
            }
        });
}

/// The middle half, through the default slice path, which decodes the whole array first.
#[divan::bench(args = SHAPES)]
fn slice_decode(bencher: Bencher, shape: Shape) {
    let array = encoded(shape);
    bencher
        .with_inputs(|| (array.clone(), SESSION.create_execution_ctx()))
        .bench_local_values(|(array, mut ctx)| {
            let sliced = array.slice(LEN / 4..3 * LEN / 4).unwrap();
            divan::black_box(sliced.execute::<PrimitiveArray>(&mut ctx).unwrap());
        });
}

fn main() {
    divan::main();
}
