// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::unwrap_used)]
#![expect(clippy::cast_possible_truncation)]

use std::sync::LazyLock;

use divan::Bencher;
use mimalloc::MiMalloc;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::array_session;
use vortex_array::patches::Patches;
use vortex_buffer::BitBuffer;
use vortex_buffer::Buffer;
use vortex_mask::Mask;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    LazyLock::force(&SESSION);
    divan::main();
}

static SESSION: LazyLock<VortexSession> = LazyLock::new(array_session);

/// Sized to keep CodSpeed simulation under 1ms per benchmark.
const ARRAY_LEN: usize = 4096;

const BENCH_ARGS: &[(f64, f64)] = &[
    // patches_density, mask_density
    (0.1, 0.9),
    (0.1, 0.5),
    (0.1, 0.01),
    (0.01, 0.9),
    (0.01, 0.5),
    (0.01, 0.01),
    (0.001, 0.9),
    (0.001, 0.5),
];

/// Mask without cached indices, exercising the bitmap path.
#[divan::bench(args = BENCH_ARGS)]
fn filter_bitmap_mask(bencher: Bencher, (patches_density, mask_density): (f64, f64)) {
    let mut rng = StdRng::seed_from_u64(0);
    let patches = fixture(ARRAY_LEN, patches_density, &mut rng);
    let mask = Mask::from_buffer(BitBuffer::from_iter(
        (0..ARRAY_LEN).map(|_| rng.random_bool(mask_density)),
    ));

    bencher
        .with_inputs(|| (&patches, &mask, SESSION.create_execution_ctx()))
        .bench_refs(|(patches, mask, ctx)| patches.filter(mask, ctx).unwrap());
}

/// Mask with cached indices, exercising the galloping paths.
#[divan::bench(args = BENCH_ARGS)]
fn filter_indices_mask(bencher: Bencher, (patches_density, mask_density): (f64, f64)) {
    let mut rng = StdRng::seed_from_u64(0);
    let patches = fixture(ARRAY_LEN, patches_density, &mut rng);
    let mask = Mask::from_indices(
        ARRAY_LEN,
        (0..ARRAY_LEN).filter(|_| rng.random_bool(mask_density)),
    );

    bencher
        .with_inputs(|| (&patches, &mask, SESSION.create_execution_ctx()))
        .bench_refs(|(patches, mask, ctx)| patches.filter(mask, ctx).unwrap());
}

fn fixture(len: usize, density: f64, rng: &mut StdRng) -> Patches {
    let indices = (0..len)
        .filter(|_| rng.random_bool(density))
        .map(|x| x as u32)
        .collect::<Buffer<u32>>();
    let values = Buffer::from_iter((0..indices.len()).map(|x| x as u64)).into_array();
    Patches::new(len, 0, indices.into_array(), values, None).unwrap()
}
