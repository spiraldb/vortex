// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::unwrap_used)]

use std::sync::LazyLock;

use divan::Bencher;
use mimalloc::MiMalloc;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::scalar::Scalar;
use vortex_fastlanes::BitPacked;
use vortex_fastlanes::BitPackedArrayExt;
use vortex_fastlanes::BitPackedArraySlotsExt;
use vortex_fastlanes::FoR;
use vortex_fastlanes::bitpack_compress::bitpack_encode;
use vortex_fastlanes::bitpack_compress::bitpack_encode_blocked;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    LazyLock::force(&SESSION);
    divan::main();
}

const LEN: usize = 65_536;
const ACCESSES: usize = 256;
const FIXTURES: &[(&str, bool)] = &[
    ("global", false),
    ("global", true),
    ("blocked_primitive", false),
    ("blocked_primitive", true),
    ("blocked_for", false),
    ("blocked_for", true),
];
const PATCHED_FIXTURES: &[&str] = &["global", "blocked_primitive", "blocked_for"];

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    vortex_fastlanes::initialize(&session);
    session
});

fn fixture(mode: &str, patched: bool) -> ArrayRef {
    let widths: Vec<u8> = (0..LEN / 1024).map(|block| 4 + (block % 4) as u8).collect();
    let values = PrimitiveArray::from_iter((0..LEN).map(|index| {
        let width = widths[index / 1024];
        if patched && index % 64 == 0 {
            65_535u32
        } else {
            ((index * 7919) as u32) & ((1u32 << width) - 1)
        }
    }));
    let mut ctx = SESSION.create_execution_ctx();
    let encoded = if mode == "global" {
        bitpack_encode(&values, 7, None, &mut ctx).unwrap()
    } else {
        bitpack_encode_blocked(&values, &widths, None, &mut ctx).unwrap()
    };
    assert_eq!(encoded.patches().is_some(), patched);
    if mode == "blocked_for" {
        let offsets = encoded.block_offsets().unwrap().clone();
        let reference = Scalar::zero_value(offsets.dtype());
        let offsets = FoR::try_new(offsets, reference).unwrap().into_array();
        BitPacked::try_new_with_block_offsets(
            encoded.packed().clone(),
            encoded.dtype().as_ptype(),
            encoded.validity().unwrap(),
            encoded.patches(),
            offsets,
            encoded.len(),
            encoded.offset(),
        )
        .unwrap()
        .into_array()
    } else {
        encoded.into_array()
    }
}

fn random_index(read: usize) -> usize {
    (read * 7919) % LEN
}

#[divan::bench(args = FIXTURES)]
fn random_once(bencher: Bencher, (mode, patched): (&str, bool)) {
    let array = fixture(mode, patched);
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            for read in 0..ACCESSES {
                divan::black_box(array.execute_scalar(random_index(read), ctx).unwrap());
            }
        });
}

#[divan::bench(args = FIXTURES)]
fn random_repeated(bencher: Bencher, (mode, patched): (&str, bool)) {
    let array = fixture(mode, patched);
    bencher
        .with_inputs(|| (SESSION.create_execution_ctx(), array.repeated_probe()))
        .bench_refs(|(ctx, probe)| {
            for read in 0..ACCESSES {
                divan::black_box(probe.execute_scalar(random_index(read), ctx).unwrap());
            }
        });
}

#[divan::bench(args = FIXTURES)]
fn same_block_once(bencher: Bencher, (mode, patched): (&str, bool)) {
    let array = fixture(mode, patched);
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            for read in 0..ACCESSES {
                divan::black_box(array.execute_scalar(1024 + read, ctx).unwrap());
            }
        });
}

#[divan::bench(args = FIXTURES)]
fn same_block_repeated(bencher: Bencher, (mode, patched): (&str, bool)) {
    let array = fixture(mode, patched);
    bencher
        .with_inputs(|| (SESSION.create_execution_ctx(), array.repeated_probe()))
        .bench_refs(|(ctx, probe)| {
            for read in 0..ACCESSES {
                divan::black_box(probe.execute_scalar(1024 + read, ctx).unwrap());
            }
        });
}

#[divan::bench(args = PATCHED_FIXTURES)]
fn patch_hit_repeated(bencher: Bencher, mode: &str) {
    let array = fixture(mode, true);
    bencher
        .with_inputs(|| (SESSION.create_execution_ctx(), array.repeated_probe()))
        .bench_refs(|(ctx, probe)| {
            for _ in 0..ACCESSES {
                divan::black_box(probe.execute_scalar(1024, ctx).unwrap());
            }
        });
}

#[divan::bench(args = PATCHED_FIXTURES)]
fn patch_miss_repeated(bencher: Bencher, mode: &str) {
    let array = fixture(mode, true);
    bencher
        .with_inputs(|| (SESSION.create_execution_ctx(), array.repeated_probe()))
        .bench_refs(|(ctx, probe)| {
            for _ in 0..ACCESSES {
                divan::black_box(probe.execute_scalar(1025, ctx).unwrap());
            }
        });
}
