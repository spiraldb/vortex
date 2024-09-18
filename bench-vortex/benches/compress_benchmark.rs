use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::*;
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::{compress_taxi_data, tpch};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkGroup, Criterion};
use vortex::{Array, IntoArray, IntoCanonical};
use vortex_sampling_compressor::compressors::fsst::FSSTCompressor;
use vortex_sampling_compressor::SamplingCompressor;

fn benchmark_compress<'a, T: criterion::measurement::Measurement, F>(
    compressor: &SamplingCompressor<'_>,
    make_uncompressed: F,
    group_name: &str,
    group: &mut BenchmarkGroup<'_, T>,
    bench_name: &str,
) where
    F: Fn() -> &'a Array,
{
    let mut uncompressed_size = 0;
    let mut compressed_size = 0;

    group.bench_function(bench_name, |b| {
        b.iter_with_large_drop(|| {
            let uncompressed = make_uncompressed();
            uncompressed_size = uncompressed.nbytes();
            let compressed_array =
                std::hint::black_box(compressor.compress(uncompressed, None)).unwrap();
            compressed_size = compressed_array.nbytes();
        });
    });

    println!(
        "test {} Compression Ratio/{} ... bench:    {} ratio (+/- 0)",
        group_name,
        bench_name,
        (compressed_size as f64) / (uncompressed_size as f64),
    );
}

fn yellow_taxi_trip_data(c: &mut Criterion) {
    taxi_data_parquet();
    let mut group = c.benchmark_group("Yellow Taxi Trip Data");
    group.sample_size(10);
    group.bench_function("compress", |b| b.iter(|| black_box(compress_taxi_data())));
    group.finish()
}

fn public_bi_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Public BI Benchmark");
    group.sample_size(10);
    // group.measurement_time(Duration::new(10, 0));

    for dataset_name in [
        AirlineSentiment,
        Arade,
        // Bimbo, // 27s per sample
        // CMSprovider, // >30s per sample
        // Corporations, // duckdb thinks ' is a quote character but its used as an apostrophe
        // CityMaxCapita, // 11th column has F, M, and U but is inferred as boolean
        Euro2016,
        Food,
        HashTags,
        // Hatred, // panic in fsst_compress_iter
        // TableroSistemaPenal, // 20s per sample
        // YaleLanguages, // 4th column looks like integer but also contains Y
    ] {
        group.bench_function(format!("{:?}", dataset_name), |b| {
            let dataset = BenchmarkDatasets::PBI(dataset_name);
            dataset.write_as_parquet();
            b.iter(|| black_box(dataset.compress_to_vortex()))
        });
    }
    group.finish()
}

fn tpc_h_l_comment(c: &mut Criterion) {
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

    let group_name = "TPC-H l_comment";
    let mut group = c.benchmark_group(format!("{} Compression Time", group_name));
    group.sample_size(10);
    // group.measurement_time(Duration::new(15, 0));

    let comments = lineitem_vortex.with_dyn(|a| {
        a.as_struct_array_unchecked()
            .field_by_name("l_comment")
            .unwrap()
    });

    benchmark_compress(
        &compressor,
        || &comments,
        group_name,
        &mut group,
        "chunked-without-fsst",
    );

    benchmark_compress(
        &compressor_fsst,
        || &comments,
        group_name,
        &mut group,
        "chunked-with-fsst",
    );

    let comments_canonical = comments
        .into_canonical()
        .unwrap()
        .into_varbin()
        .unwrap()
        .into_array();

    benchmark_compress(
        &compressor_fsst,
        || &comments_canonical,
        group_name,
        &mut group,
        "canonical-with-fsst",
    );

    group.finish();
}

criterion_group!(
    benches,
    yellow_taxi_trip_data,
    public_bi_benchmark,
    tpc_h_l_comment,
);
criterion_main!(benches);
