// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::unwrap_used)]
#![expect(clippy::cast_possible_truncation)]

use divan::Bencher;
use mimalloc::MiMalloc;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::DictArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_mask::Mask;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    divan::main();
}

// Sized to keep CodSpeed simulation under 1ms per benchmark.
const LEN: usize = 16_384;
const NUM_VALUES: usize = 256;
const DENSITIES: &[f64] = &[0.01, 0.1, 0.5, 0.9];

fn filter_mask(density: f64) -> Mask {
    let mut rng = StdRng::seed_from_u64(1);
    Mask::from_iter((0..LEN).map(|_| rng.random_bool(density)))
}

fn random_codes() -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(0);
    PrimitiveArray::from_iter((0..LEN).map(|_| rng.random_range(0..NUM_VALUES) as u8)).into_array()
}

fn primitive_values() -> ArrayRef {
    PrimitiveArray::from_iter((0..NUM_VALUES as i64).map(|i| i * 1_000)).into_array()
}

fn string_values() -> ArrayRef {
    VarBinViewArray::from_iter_str((0..NUM_VALUES).map(|i| format!("dictionary value {i:04}")))
        .into_array()
}

fn bench_filter(bencher: Bencher, array: ArrayRef, density: f64) {
    let mask = filter_mask(density);
    let session = vortex_array::array_session();
    bencher
        .with_inputs(|| (&array, &mask, session.create_execution_ctx()))
        .bench_refs(|(array, mask, ctx)| {
            array
                .filter((*mask).clone())
                .unwrap()
                .execute::<Canonical>(ctx)
                .unwrap()
        });
}

#[divan::bench(args = DENSITIES)]
fn filter_primitive_values(bencher: Bencher, density: f64) {
    let array = DictArray::try_new(random_codes(), primitive_values())
        .unwrap()
        .into_array();
    bench_filter(bencher, array, density);
}

#[divan::bench(args = DENSITIES)]
fn filter_string_values(bencher: Bencher, density: f64) {
    let array = DictArray::try_new(random_codes(), string_values())
        .unwrap()
        .into_array();
    bench_filter(bencher, array, density);
}

#[divan::bench(args = DENSITIES)]
fn filter_constant_codes_primitive_values(bencher: Bencher, density: f64) {
    let array = DictArray::try_new(
        ConstantArray::new(7u8, LEN).into_array(),
        primitive_values(),
    )
    .unwrap()
    .into_array();
    bench_filter(bencher, array, density);
}

#[divan::bench(args = DENSITIES)]
fn filter_constant_codes_string_values(bencher: Bencher, density: f64) {
    let array = DictArray::try_new(ConstantArray::new(7u8, LEN).into_array(), string_values())
        .unwrap()
        .into_array();
    bench_filter(bencher, array, density);
}
