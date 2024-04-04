use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fastlanez::TryBitPack;
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::take::take;
use vortex::encoding::EncodingRef;
use vortex_fastlanes::{
    bitpack_primitive, unpack_primitive, unpack_single_primitive, BitPackedEncoding,
    DowncastFastlanes,
};

fn values(len: usize, bits: usize) -> Vec<u32> {
    let rng = thread_rng();
    let range = Uniform::new(0_u32, 2_u32.pow(bits as u32));
    rng.sample_iter(range).take(len).collect()
}

fn unpack_singles(packed: &[u8], bit_width: usize, length: usize) -> Vec<u32> {
    let mut output = Vec::with_capacity(length);
    for i in 0..length {
        unsafe {
            output.push(unpack_single_primitive(packed, bit_width, i).unwrap());
        }
    }
    output
}

fn pack_unpack(c: &mut Criterion) {
    let bits: usize = 8;
    let values = values(1_000_000, bits);

    c.bench_function("bitpack_1M", |b| {
        b.iter(|| black_box(bitpack_primitive(&values, bits)));
    });

    let packed = bitpack_primitive(&values, bits);
    let unpacked = unpack_primitive::<u32>(&packed, bits, values.len());
    assert_eq!(unpacked, values);

    c.bench_function("unpack_1M", |b| {
        b.iter(|| black_box(unpack_primitive::<u32>(&packed, bits, values.len())));
    });

    c.bench_function("unpack_1M_singles", |b| {
        b.iter(|| black_box(unpack_singles(&packed, 8, values.len())));
    });

    // 1024 elements pack into `128 * bits` bytes
    let packed_1024 = &packed[0..128 * bits];
    c.bench_function("unpack_1024_alloc", |b| {
        b.iter(|| black_box(unpack_primitive::<u32>(&packed, bits, values.len())));
    });

    let mut output: Vec<u32> = Vec::with_capacity(1024);
    c.bench_function("unpack_1024_noalloc", |b| {
        b.iter(|| {
            output.clear();
            TryBitPack::try_unpack_into(packed_1024, bits, &mut output).unwrap();
            black_box(output[0])
        })
    });

    c.bench_function("unpack_single", |b| {
        b.iter(|| black_box(unsafe { unpack_single_primitive::<u32>(packed_1024, 8, 0) }));
    });
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

fn bench_take_patched(c: &mut Criterion) {
    let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
    let ctx = CompressCtx::new(Arc::new(cfg));

    let big_base2 = 1048576;
    let num_exceptions = 100;
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

    let not_patch_indices: PrimitiveArray = (0u32..10000).collect_vec().into();
    c.bench_function("take_10K_not_patches", |b| {
        b.iter(|| black_box(take(packed, &not_patch_indices).unwrap()));
    });

    let patch_indices: PrimitiveArray = (big_base2..big_base2 + num_exceptions)
        .cycle()
        .take(10000)
        .collect_vec()
        .into();
    c.bench_function("take_10K_patches", |b| {
        b.iter(|| black_box(take(packed, &patch_indices).unwrap()));
    });
}

criterion_group!(benches, pack_unpack, bench_take, bench_take_patched);
criterion_main!(benches);
