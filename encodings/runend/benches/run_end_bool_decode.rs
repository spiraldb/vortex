// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Decoding boolean runs with explicit values versus normalized, alternating runs.

#![expect(clippy::cast_possible_truncation)]

use std::sync::LazyLock;

use divan::Bencher;
use mimalloc::MiMalloc;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::validity::Validity;
use vortex_buffer::BitBuffer;
use vortex_buffer::Buffer;
use vortex_runend::RunEnd;
use vortex_runend::RunEndBool;
use vortex_runend::decompress_bool::decode_alternating_bools;
use vortex_runend::decompress_bool::runend_decode_bools;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    divan::main();
}

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    vortex_runend::initialize(&session);
    session
});

const LEN: usize = 1 << 20;

/// `(avg_run_length, p_repeat)`: `p_repeat` is the chance that a run repeats the previous run's
/// value, as a compare pushed into run values produces, e.g. `[t, t, f]`.
const ARGS: &[(usize, f64)] = &[
    (4, 0.0),
    (4, 0.5),
    (4, 0.9),
    (32, 0.0),
    (32, 0.5),
    (32, 0.9),
    (512, 0.5),
];

struct Data {
    ends: Vec<u32>,
    values: BitBuffer,
    merged_ends: Vec<u32>,
    start: bool,
}

fn data(avg_run: usize, p_repeat: f64) -> Data {
    let mut rng = StdRng::seed_from_u64(42);
    let mut ends = Vec::new();
    let mut values = Vec::new();
    let mut pos = 0usize;
    let mut value = false;
    while pos < LEN {
        pos = (pos + rng.random_range(1..=2 * avg_run - 1)).min(LEN);
        if !rng.random_bool(p_repeat) {
            value = !value;
        }
        ends.push(pos as u32);
        values.push(value);
    }
    let mut merged_ends: Vec<u32> = Vec::new();
    let mut prev = None;
    for (&end, &v) in ends.iter().zip(&values) {
        if prev == Some(v) {
            *merged_ends.last_mut().unwrap() = end;
        } else {
            merged_ends.push(end);
            prev = Some(v);
        }
    }
    Data {
        ends,
        start: values[0],
        values: BitBuffer::from_iter(values),
        merged_ends,
    }
}

/// Today's decoder on explicit values.
#[divan::bench(args = ARGS)]
fn explicit(bencher: Bencher, (avg_run, p_repeat): (usize, f64)) {
    let d = data(avg_run, p_repeat);
    let ends = PrimitiveArray::new(Buffer::from(d.ends), Validity::NonNullable);
    let values = BoolArray::new(d.values, Validity::NonNullable);
    bencher
        .with_inputs(|| (ends.clone(), values.clone(), SESSION.create_execution_ctx()))
        .bench_refs(|(ends, values, ctx)| {
            runend_decode_bools(ends.clone(), values.clone(), 0, LEN, ctx).unwrap()
        });
}

/// Merge adjacent equal runs at decode time, then decode them as alternating.
#[divan::bench(args = ARGS)]
fn merge_then_alternating(bencher: Bencher, (avg_run, p_repeat): (usize, f64)) {
    let d = data(avg_run, p_repeat);
    bencher.bench(|| {
        let mut merged = Vec::with_capacity(d.ends.len());
        let mut prev = None;
        for (&end, v) in d.ends.iter().zip(d.values.iter()) {
            if prev == Some(v) {
                *merged.last_mut().unwrap() = end;
            } else {
                merged.push(end);
                prev = Some(v);
            }
        }
        decode_alternating_bools(merged.iter().map(|&e| e as usize), d.start, LEN)
    });
}

/// Runs stored already normalized, as `RunEndBool` holds them.
#[divan::bench(args = ARGS)]
fn stored_alternating(bencher: Bencher, (avg_run, p_repeat): (usize, f64)) {
    let d = data(avg_run, p_repeat);
    bencher.bench(|| {
        decode_alternating_bools(d.merged_ends.iter().map(|&e| e as usize), d.start, LEN)
    });
}

/// Execute a boolean `RunEnd` with explicit values to a `BoolArray`.
#[divan::bench(args = ARGS)]
fn execute_run_end(bencher: Bencher, (avg_run, p_repeat): (usize, f64)) {
    let d = data(avg_run, p_repeat);
    let mut ctx = SESSION.create_execution_ctx();
    let array = RunEnd::try_new(
        PrimitiveArray::new(Buffer::from(d.ends), Validity::NonNullable).into_array(),
        BoolArray::new(d.values, Validity::NonNullable).into_array(),
        &mut ctx,
    )
    .unwrap()
    .into_array();
    bencher
        .with_inputs(|| (array.clone(), SESSION.create_execution_ctx()))
        .bench_refs(|(array, ctx)| array.clone().execute::<BoolArray>(ctx).unwrap());
}

/// Execute the same data, normalized into a `RunEndBool`, to a `BoolArray`.
#[divan::bench(args = ARGS)]
fn execute_run_end_bool(bencher: Bencher, (avg_run, p_repeat): (usize, f64)) {
    let d = data(avg_run, p_repeat);
    let mut ctx = SESSION.create_execution_ctx();
    let array = RunEndBool::try_new(
        PrimitiveArray::new(Buffer::from(d.merged_ends), Validity::NonNullable).into_array(),
        d.start,
        Validity::NonNullable,
        &mut ctx,
    )
    .unwrap()
    .into_array();
    bencher
        .with_inputs(|| (array.clone(), SESSION.create_execution_ctx()))
        .bench_refs(|(array, ctx)| array.clone().execute::<BoolArray>(ctx).unwrap());
}
