use std::sync::Arc;

use criterion::{black_box, Criterion, criterion_group, criterion_main};
use itertools::Itertools;
use rand::{Rng, thread_rng};
use rand::distributions::Uniform;

use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::take::take;
use vortex::encoding::EncodingRef;
use vortex_fastlanes::{BitPackedEncoding, DowncastFastlanes};

fn values(len: usize, bits: usize) -> Vec<u32> {
    let rng = thread_rng();
    let range = Uniform::new(0_u32, 2_u32.pow(bits as u32));
    rng.sample_iter(range).take(len).collect()
}

fn bench_take(c: &mut Criterion) {
    let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
    let ctx = CompressCtx::new(Arc::new(cfg));

    let values = values(1_000_000, 8);
    let uncompressed = PrimitiveArray::from(values.clone());
    let packed = BitPackedEncoding {}
        .compress(&uncompressed, None, ctx)
        .unwrap();

    let stratified_indices: PrimitiveArray = (0..10).map(|i| i * 10_000).collect::<Vec<_>>().into();
    c.bench_function("take_10_stratified", |b| {
        b.iter(|| black_box(take(&packed, &stratified_indices).unwrap()));
    });

    let contiguous_indices: PrimitiveArray = (0..10).collect::<Vec<_>>().into();
    c.bench_function("take_10_contiguous", |b| {
        b.iter(|| black_box(take(&packed, &contiguous_indices).unwrap()));
    });

    let rng = thread_rng();
    let range = Uniform::new(0, values.len());
    let random_indices: PrimitiveArray = rng
        .sample_iter(range)
        .take(10_000)
        .map(|i| i as u32)
        .collect_vec()
        .into();
    c.bench_function("take_10K_random", |b| {
        b.iter(|| black_box(take(&packed, &random_indices).unwrap()));
    });

    let contiguous_indices: PrimitiveArray = (0..10_000).collect::<Vec<_>>().into();
    c.bench_function("take_10K_contiguous", |b| {
        b.iter(|| black_box(take(&packed, &contiguous_indices).unwrap()));
    });
}

fn bench_patched_take(c: &mut Criterion) {
    let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
    let ctx = CompressCtx::new(Arc::new(cfg));

    let big_base2 = 1048576;
    let num_exceptions = 10000;
    let values = (0u32..big_base2 + num_exceptions).collect_vec();

    let uncompressed = PrimitiveArray::from(values.clone());
    let packed = BitPackedEncoding {}
        .compress(&uncompressed, None, ctx)
        .unwrap();
    let packed = packed.as_bitpacked();
    assert!(packed.patches().is_some());
    assert_eq!(
        packed.patches().unwrap().as_sparse().values().len(),
        num_exceptions as usize
    );

    let stratified_indices: PrimitiveArray = (0..10).map(|i| i * 10_000).collect::<Vec<_>>().into();
    c.bench_function("patched_take_10_stratified", |b| {
        b.iter(|| black_box(take(packed, &stratified_indices).unwrap()));
    });

    let contiguous_indices: PrimitiveArray = (0..10).collect::<Vec<_>>().into();
    c.bench_function("patched_take_10_contiguous", |b| {
        b.iter(|| black_box(take(packed, &contiguous_indices).unwrap()));
    });

    let rng = thread_rng();
    let range = Uniform::new(0, values.len());
    let random_indices: PrimitiveArray = rng
        .sample_iter(range)
        .take(10_000)
        .map(|i| i as u32)
        .collect_vec()
        .into();
    c.bench_function("patched_take_10K_random", |b| {
        b.iter(|| black_box(take(packed, &random_indices).unwrap()));
    });

    let not_patch_indices: PrimitiveArray = (0u32..num_exceptions)
        .cycle()
        .take(10000)
        .collect_vec()
        .into();
    c.bench_function("patched_take_10K_contiguous_not_patches", |b| {
        b.iter(|| black_box(take(packed, &not_patch_indices).unwrap()));
    });

    let patch_indices: PrimitiveArray = (big_base2..big_base2 + num_exceptions)
        .cycle()
        .take(10000)
        .collect_vec()
        .into();
    c.bench_function("patched_take_10K_contiguous_patches", |b| {
        b.iter(|| black_box(take(packed, &patch_indices).unwrap()));
    });
}

criterion_group!(benches, bench_take, bench_patched_take);
criterion_main!(benches);
