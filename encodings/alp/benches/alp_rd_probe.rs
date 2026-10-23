// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::unwrap_used)]

use std::sync::LazyLock;

use divan::Bencher;
use mimalloc::MiMalloc;
use vortex_alp::ALPRDArrayExt;
use vortex_alp::ALPRDFloat;
use vortex_alp::RDEncoder;
use vortex_alp::RDEncoderExt;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::NativePType;
use vortex_buffer::Buffer;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    LazyLock::force(&SESSION);
    divan::main();
}

const LEN: usize = 65_536;
const ACCESSES: usize = 256;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    vortex_alp::initialize(&session);
    session
});

fn array<T: ALPRDFloat + NativePType>(patched: bool) -> ArrayRef {
    let a = T::from(0.1).unwrap();
    let b = T::from(0.2).unwrap();
    let outlier = T::from(1e25).unwrap();
    let values = (0..LEN)
        .map(|index| {
            if patched && index % 16 == 0 {
                outlier
            } else if index % 2 == 0 {
                a
            } else {
                b
            }
        })
        .collect::<Buffer<T>>();
    let primitive = PrimitiveArray::from_iter(values);
    let encoded = RDEncoder::new(&[a, b]).encode(primitive.as_view());
    assert_eq!(encoded.left_parts_patches().is_some(), patched);
    encoded.into_array()
}

fn random_index(read: usize) -> usize {
    (read * 7919) % LEN
}

#[divan::bench(types = [f32, f64], args = [false, true])]
fn random_once<T: ALPRDFloat + NativePType>(bencher: Bencher, patched: bool) {
    let array = array::<T>(patched);
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            for read in 0..ACCESSES {
                divan::black_box(array.execute_scalar(random_index(read), ctx).unwrap());
            }
        });
}

#[divan::bench(types = [f32, f64], args = [false, true])]
fn random_repeated<T: ALPRDFloat + NativePType>(bencher: Bencher, patched: bool) {
    let array = array::<T>(patched);
    bencher
        .with_inputs(|| (SESSION.create_execution_ctx(), array.repeated_probe()))
        .bench_refs(|(ctx, probe)| {
            for read in 0..ACCESSES {
                divan::black_box(probe.execute_scalar(random_index(read), ctx).unwrap());
            }
        });
}

#[divan::bench(types = [f32, f64], args = [false, true])]
fn same_row_once<T: ALPRDFloat + NativePType>(bencher: Bencher, patched: bool) {
    let array = array::<T>(patched);
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            for _ in 0..ACCESSES {
                divan::black_box(array.execute_scalar(1, ctx).unwrap());
            }
        });
}

#[divan::bench(types = [f32, f64], args = [false, true])]
fn same_row_repeated<T: ALPRDFloat + NativePType>(bencher: Bencher, patched: bool) {
    let array = array::<T>(patched);
    bencher
        .with_inputs(|| (SESSION.create_execution_ctx(), array.repeated_probe()))
        .bench_refs(|(ctx, probe)| {
            for _ in 0..ACCESSES {
                divan::black_box(probe.execute_scalar(1, ctx).unwrap());
            }
        });
}

#[divan::bench(types = [f32, f64], args = [false, true])]
fn patch_row_once<T: ALPRDFloat + NativePType>(bencher: Bencher, hit: bool) {
    let array = array::<T>(true);
    let index = if hit { 0 } else { 1 };
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| {
            for _ in 0..ACCESSES {
                divan::black_box(array.execute_scalar(index, ctx).unwrap());
            }
        });
}

#[divan::bench(types = [f32, f64], args = [false, true])]
fn patch_row_repeated<T: ALPRDFloat + NativePType>(bencher: Bencher, hit: bool) {
    let array = array::<T>(true);
    let index = if hit { 0 } else { 1 };
    bencher
        .with_inputs(|| (SESSION.create_execution_ctx(), array.repeated_probe()))
        .bench_refs(|(ctx, probe)| {
            for _ in 0..ACCESSES {
                divan::black_box(probe.execute_scalar(index, ctx).unwrap());
            }
        });
}
