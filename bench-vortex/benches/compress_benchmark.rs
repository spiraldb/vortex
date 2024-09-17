use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::*;
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::{compress_taxi_data, tpch};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use vortex::{IntoArray, IntoCanonical};
use vortex_sampling_compressor::compressors::fsst::FSSTCompressor;
use vortex_sampling_compressor::SamplingCompressor;

fn vortex_compress_taxi(c: &mut Criterion) {
    taxi_data_parquet();
    let mut group = c.benchmark_group("Yellow Taxi Trip Data");
    group.sample_size(10);
    group.bench_function("compress", |b| b.iter(|| black_box(compress_taxi_data())));
    group.finish()
}

fn vortex_compress_medicare1(c: &mut Criterion) {
    let mut group = c.benchmark_group("Public BI Benchmark");
    group.sample_size(10);

    for dataset in [
        Arade,
        CityMaxCapita,
        Euro2016,
        Food,
        HashTags,
        Hatred,
        TableroSistemaPenal,
        YaleLanguages,
    ] {
        let dataset = BenchmarkDatasets::PBI(dataset);
        dataset.write_as_parquet();
        group.bench_function("compress", |b| {
            b.iter(|| black_box(dataset.compress_to_vortex()))
        });
    }
    group.finish()
}

fn vortex_compress_tpch_l_comment(c: &mut Criterion) {
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let lineitem_vortex = rt.block_on(tpch::load_table(
        data_dir,
        "lineitem",
        &tpch::schema::LINEITEM,
    ));

    let compressor = SamplingCompressor::default().excluding(&FSSTCompressor);

    let compressor_fsst = SamplingCompressor::default();

    // l_comment column only
    let mut group = c.benchmark_group("TPCH l_comment Column");
    let comments = lineitem_vortex.with_dyn(|a| {
        a.as_struct_array_unchecked()
            .field_by_name("l_comment")
            .unwrap()
    });

    group.sample_size(10);
    group.bench_function("compress-default", |b| {
        b.iter_with_large_drop(|| {
            std::hint::black_box(compressor.compress(&comments, None)).unwrap()
        });
    });

    group.bench_function("compress-fsst-chunked", |b| {
        b.iter_with_large_drop(|| {
            std::hint::black_box(compressor_fsst.compress(&comments, None)).unwrap()
        });
    });

    // Compare canonicalizing
    let comments_canonical = comments
        .into_canonical()
        .unwrap()
        .into_varbin()
        .unwrap()
        .into_array();
    group.bench_function("compress-fsst-canonicalized", |b| {
        b.iter_with_large_drop(|| {
            std::hint::black_box(compressor_fsst.compress(&comments_canonical, None)).unwrap()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    vortex_compress_taxi,
    vortex_compress_medicare1,
    vortex_compress_tpch
);
criterion_main!(benches);
