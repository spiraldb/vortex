// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Benchmarks filtering bit-packed arrays: selective filtering around the sparse extraction
//! thresholds, random selections, and selections of the elements of a `FixedSizeList<i32>`,
//! where the list-level selection expands into runs of selected elements.

#![expect(clippy::cast_possible_truncation)]
#![expect(clippy::unwrap_used)]

use std::sync::LazyLock;

use divan::Bencher;
use divan::counter::ItemsCount;
use mimalloc::MiMalloc;
use rand::RngExt;
use rand::SeedableRng;
use rand::prelude::StdRng;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::RecursiveCanonical;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::FixedSizeListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::validity::Validity;
use vortex_buffer::BitBuffer;
use vortex_buffer::BufferMut;
use vortex_fastlanes::BitPackedData;
use vortex_mask::Mask;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    divan::main();
}

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    vortex_fastlanes::initialize(&session);
    session
});

const BIT_WIDTH: u8 = 16;

const NUM_ARRAY_CHUNKS: usize = 64;
// Keep the array density below the outer full-decode policy.
const NUM_SELECTED_CHUNKS: usize = 8;
const CHUNK_SIZE: usize = 1_024;
const LEN: usize = NUM_ARRAY_CHUNKS * CHUNK_SIZE;

const NUM_ELEMENTS: usize = 1 << 17;

fn sparse_fixture(selected_per_chunk: usize) -> (ArrayRef, Mask) {
    let values: BufferMut<u32> = (0..LEN)
        .map(|index| (index % (1 << BIT_WIDTH)) as u32)
        .collect();
    let packed = BitPackedData::encode(
        &PrimitiveArray::new(values.freeze(), Validity::NonNullable).into_array(),
        BIT_WIDTH,
        &mut SESSION.create_execution_ctx(),
    )
    .unwrap()
    .into_array();

    let indices = (0..NUM_SELECTED_CHUNKS).flat_map(|chunk| {
        (0..selected_per_chunk)
            .map(move |index| chunk * CHUNK_SIZE + index * CHUNK_SIZE / selected_per_chunk)
    });
    (packed, Mask::from_indices(LEN, indices))
}

// Selection counts around the sparse extraction threshold for 16-bit values.
#[vortex_bench_support::cpu_features]
#[divan::bench(args = [8, 64, 80])]
fn filter(bencher: Bencher, selected_per_chunk: usize) {
    let (packed, mask) = sparse_fixture(selected_per_chunk);

    bencher
        .counter(ItemsCount::new(LEN))
        .with_inputs(|| (mask.clone(), SESSION.create_execution_ctx()))
        .bench_refs(|(mask, ctx)| {
            packed
                .filter(mask.clone())
                .unwrap()
                .execute::<PrimitiveArray>(ctx)
                .unwrap()
        });
}

fn bitpacked_i32(len: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(0);
    let values = (0..len)
        .map(|_| rng.random_range(0..(1i32 << BIT_WIDTH)))
        .collect::<BufferMut<i32>>();
    let array = PrimitiveArray::new(values, Validity::NonNullable).into_array();
    BitPackedData::encode(&array, BIT_WIDTH, &mut SESSION.create_execution_ctx())
        .unwrap()
        .into_array()
}

fn random_bits(len: usize, density: f64) -> BitBuffer {
    let mut rng = StdRng::seed_from_u64(1);
    BitBuffer::from_iter((0..len).map(|_| rng.random_bool(density)))
}

fn bench_filter(bencher: Bencher, array: &ArrayRef, bits: &BitBuffer) {
    // Build a fresh mask for every iteration so that representations cached on the mask, such as
    // its indices, are not reused across iterations.
    bencher
        .with_inputs(|| {
            (
                array,
                Mask::from_buffer(bits.clone()),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_values(|(array, mask, mut ctx)| {
            array
                .filter(mask)
                .unwrap()
                .execute::<RecursiveCanonical>(&mut ctx)
                .unwrap()
        });
}

#[divan::bench(args = [0.01, 0.1])]
fn primitive_i32(bencher: Bencher, density: f64) {
    let array = bitpacked_i32(NUM_ELEMENTS);
    bench_filter(bencher, &array, &random_bits(NUM_ELEMENTS, density));
}

#[divan::bench(consts = [4, 16], args = [0.1])]
fn fsl_i32<const LIST_SIZE: u32>(bencher: Bencher, density: f64) {
    let len = NUM_ELEMENTS / LIST_SIZE as usize;
    let elements = bitpacked_i32(NUM_ELEMENTS);
    let array =
        FixedSizeListArray::new(elements, LIST_SIZE, Validity::NonNullable, len).into_array();
    bench_filter(bencher, &array, &random_bits(len, density));
}
