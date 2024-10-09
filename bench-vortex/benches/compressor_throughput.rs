use std::collections::HashSet;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use itertools::Itertools as _;
use mimalloc::MiMalloc;
use vortex::array::PrimitiveArray;
use vortex::compute::unary::try_cast;
use vortex::validity::Validity;
use vortex::{IntoArray as _, IntoCanonical};
use vortex_dtype::PType;
use vortex_sampling_compressor::compressors::alp::ALPCompressor;
use vortex_sampling_compressor::compressors::alp_rd::ALPRDCompressor;
use vortex_sampling_compressor::compressors::bitpacked::BitPackedCompressor;
use vortex_sampling_compressor::compressors::delta::DeltaCompressor;
use vortex_sampling_compressor::compressors::dict::DictCompressor;
use vortex_sampling_compressor::compressors::r#for::FoRCompressor;
use vortex_sampling_compressor::compressors::roaring_int::RoaringIntCompressor;
use vortex_sampling_compressor::compressors::runend::DEFAULT_RUN_END_COMPRESSOR;
use vortex_sampling_compressor::compressors::zigzag::ZigZagCompressor;
use vortex_sampling_compressor::compressors::CompressorRef;
use vortex_sampling_compressor::SamplingCompressor;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn primitive(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitive-decompression");
    let num_values = u16::MAX as u64;
    group.throughput(Throughput::Bytes(num_values * 4));

    let int_array = PrimitiveArray::from_vec(
        (0..num_values).map(|i| i as u32 + 100).collect_vec(),
        Validity::NonNullable,
    )
    .into_array();

    const UINT_COMPRESSORS: [CompressorRef<'static>; 5] = [
        &BitPackedCompressor,
        &DEFAULT_RUN_END_COMPRESSOR,
        &DeltaCompressor,
        &DictCompressor,
        &RoaringIntCompressor,
    ];
    for compressor in UINT_COMPRESSORS {
        group.bench_function(format!("{} compress", compressor.id()), |b| {
            b.iter(|| {
                let ctx = SamplingCompressor::new(HashSet::from([compressor]));
                black_box(compressor.compress(&int_array, None, ctx).unwrap());
            })
        });

        let ctx = SamplingCompressor::new(HashSet::from([compressor]));
        let compressed = compressor.compress(&int_array, None, ctx).unwrap().into_array();
        group.bench_function(format!("{} decompress", compressor.id()), |b| {
            b.iter(|| {
                black_box(compressed.clone().into_canonical().unwrap());
            })
        });
    }

    const SIGNED_INT_COMPRESSORS: [CompressorRef<'static>; 2] = [
        &FoRCompressor,
        &ZigZagCompressor,
    ];
    let int_array = try_cast(int_array, PType::I32.into()).unwrap();
    for compressor in SIGNED_INT_COMPRESSORS {
        group.bench_function(format!("{} compress", compressor.id()), |b| {
            b.iter(|| {
                let ctx = SamplingCompressor::new(HashSet::from([compressor]));
                black_box(compressor.compress(&int_array, None, ctx).unwrap());
            })
        });

        let ctx = SamplingCompressor::new(HashSet::from([compressor]));
        let compressed = compressor.compress(&int_array, None, ctx).unwrap().into_array();
        group.bench_function(format!("{} decompress", compressor.id()), |b| {
            b.iter(|| {
                black_box(compressed.clone().into_canonical().unwrap());
            })
        });
    }

    let float_array = try_cast(int_array, PType::F32.into()).unwrap();
    const FLOAT_COMPRESSORS: [CompressorRef<'static>; 2] = [&ALPCompressor, &ALPRDCompressor];
    for compressor in FLOAT_COMPRESSORS {
        group.bench_function(format!("{} compress", compressor.id()), |b| {
            b.iter(|| {
                let ctx = SamplingCompressor::new(HashSet::from([compressor]));
                black_box(compressor.compress(&float_array, None, ctx).unwrap());
            })
        });

        let ctx = SamplingCompressor::new(HashSet::from([compressor]));
        let compressed = compressor.compress(&float_array, None, ctx).unwrap().into_array();
        group.bench_function(format!("{} decompress", compressor.id()), |b| {
            b.iter(|| {
                black_box(compressed.clone().into_canonical().unwrap());
            })
        });
    }
}

criterion_group!(benches, primitive);
criterion_main!(benches);
