// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Scalar access comparisons, including preparation and teardown for every group of lookups.

use std::hint::black_box;
use std::sync::LazyLock;

use divan::Bencher;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::ProbeUsage;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::validity::Validity;
use vortex_error::VortexExpect;
use vortex_fastlanes::RLEData;
use vortex_pco::Pco;
use vortex_runend::RunEnd;
use vortex_session::VortexSession;

fn main() {
    divan::main();
}

const LEN: usize = 16_384;
// Number of accesses, nullable, scattered (otherwise clustered within 256 rows).
const CASES: &[(usize, bool, bool)] = &[
    (1, false, false),
    (1, true, false),
    (64, false, false),
    (64, true, false),
    (64, false, true),
    (64, true, true),
    (1024, false, false),
    (1024, true, false),
    (1024, false, true),
    (1024, true, true),
];

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    vortex_fastlanes::initialize(&session);
    vortex_runend::initialize(&session);
    session
});

fn input(nullable: bool) -> PrimitiveArray {
    let validity = if nullable {
        Validity::from_iter((0..LEN).map(|i| i % 11 != 0))
    } else {
        Validity::NonNullable
    };
    PrimitiveArray::new(
        (0..LEN)
            .map(|i| u32::try_from(i / 16).vortex_expect("fixture values fit u32"))
            .collect::<Vec<_>>(),
        validity,
    )
}

fn indices(count: usize, scattered: bool) -> Vec<usize> {
    let span = if scattered { LEN } else { 256 };
    let base = if scattered { 0 } else { 4096 };
    let mut seed = 42u64;
    (0..count)
        .map(|_| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            base + ((seed >> 32) as usize % span)
        })
        .collect()
}

fn rle(nullable: bool) -> ArrayRef {
    let mut ctx = SESSION.create_execution_ctx();
    RLEData::encode(input(nullable).as_view(), &mut ctx)
        .vortex_expect("RLE compression")
        .into_array()
}

fn pco(nullable: bool) -> ArrayRef {
    let mut ctx = SESSION.create_execution_ctx();
    Pco::from_primitive(input(nullable).as_view(), 8, 1024, &mut ctx)
        .vortex_expect("PCO compression")
        .into_array()
}

fn runend_pco(nullable: bool) -> ArrayRef {
    let mut ctx = SESSION.create_execution_ctx();
    let runs = u32::try_from(LEN / 4).vortex_expect("run count fits u32");
    let ends = PrimitiveArray::from_iter((1..=runs).map(|run| run * 4));
    let values = PrimitiveArray::new(
        (0..runs).collect::<Vec<_>>(),
        if nullable {
            Validity::from_iter((0..runs).map(|run| run % 11 != 0))
        } else {
            Validity::NonNullable
        },
    );
    RunEnd::try_new(
        Pco::from_primitive(ends.as_view(), 8, 1024, &mut ctx)
            .vortex_expect("PCO ends compression")
            .into_array(),
        Pco::from_primitive(values.as_view(), 8, 1024, &mut ctx)
            .vortex_expect("PCO values compression")
            .into_array(),
        &mut ctx,
    )
    .vortex_expect("RunEnd construction")
    .into_array()
}

fn execute_scalar(bencher: Bencher, array: ArrayRef, indices: &[usize]) {
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            for &index in indices {
                black_box(
                    array
                        .execute_scalar(black_box(index), ctx)
                        .vortex_expect("scalar access"),
                );
            }
        });
}

fn probe(bencher: Bencher, array: ArrayRef, indices: &[usize], usage: ProbeUsage) {
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            let mut probe = array.probe(usage);
            for &index in indices {
                black_box(
                    probe
                        .scalar_at(black_box(index), ctx)
                        .vortex_expect("probe access"),
                );
            }
        });
}

#[divan::bench(args = CASES)]
fn rle_probe(bencher: Bencher, (count, nullable, scattered): (usize, bool, bool)) {
    probe(
        bencher,
        rle(nullable),
        &indices(count, scattered),
        if count == 1 {
            ProbeUsage::Once
        } else {
            ProbeUsage::Repeated
        },
    );
}

#[divan::bench(args = CASES)]
fn pco_probe(bencher: Bencher, (count, nullable, scattered): (usize, bool, bool)) {
    probe(
        bencher,
        pco(nullable),
        &indices(count, scattered),
        if count == 1 {
            ProbeUsage::Once
        } else {
            ProbeUsage::Repeated
        },
    );
}

#[divan::bench(args = [false, true])]
fn rle_repeated_first(bencher: Bencher, nullable: bool) {
    probe(
        bencher,
        rle(nullable),
        &indices(1, false),
        ProbeUsage::Repeated,
    );
}

#[divan::bench(args = [false, true])]
fn pco_repeated_first(bencher: Bencher, nullable: bool) {
    probe(
        bencher,
        pco(nullable),
        &indices(1, false),
        ProbeUsage::Repeated,
    );
}

#[divan::bench(args = CASES)]
fn rle_execute_scalar(bencher: Bencher, (count, nullable, scattered): (usize, bool, bool)) {
    execute_scalar(bencher, rle(nullable), &indices(count, scattered));
}

#[divan::bench(args = CASES)]
fn pco_execute_scalar(bencher: Bencher, (count, nullable, scattered): (usize, bool, bool)) {
    execute_scalar(bencher, pco(nullable), &indices(count, scattered));
}

#[divan::bench(args = CASES)]
fn runend_pco_execute_scalar(bencher: Bencher, (count, nullable, scattered): (usize, bool, bool)) {
    execute_scalar(bencher, runend_pco(nullable), &indices(count, scattered));
}

#[divan::bench(args = CASES)]
fn runend_pco_probe(bencher: Bencher, (count, nullable, scattered): (usize, bool, bool)) {
    probe(
        bencher,
        runend_pco(nullable),
        &indices(count, scattered),
        if count == 1 {
            ProbeUsage::Once
        } else {
            ProbeUsage::Repeated
        },
    );
}
