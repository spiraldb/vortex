use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::*;
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::{fetch_taxi_data, tpch};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkGroup, Criterion};
use vortex::{Array, IntoArray, IntoCanonical};
use vortex_sampling_compressor::compressors::fsst::FSSTCompressor;
use vortex_sampling_compressor::SamplingCompressor;

fn benchmark_compress<T: criterion::measurement::Measurement, F, U>(
    compressor: &SamplingCompressor<'_>,
    make_uncompressed: F,
    group_name: &str,
    group: &mut BenchmarkGroup<'_, T>,
    bench_name: &str,
) where
    F: Fn() -> U,
    U: AsRef<Array>,
{
    let mut uncompressed_size = 0;
    let mut uncompressed_tree: String = "".to_string();
    let mut compressed_size = 0;
    let mut compressed_tree: String = "".to_string();

    group.bench_function(bench_name, |b| {
        b.iter_with_large_drop(|| {
            let uncompressed = make_uncompressed();
            uncompressed_size = uncompressed.as_ref().nbytes();
            let compressed = black_box(compressor.compress(uncompressed.as_ref(), None)).unwrap();
            compressed_size = compressed.nbytes();
            uncompressed_tree = format!("{}", uncompressed.as_ref().tree_display());
            compressed_tree = format!("{}", compressed.as_ref().tree_display());
        });
    });

    println!(
        "test {} Compression Ratio/{} ... bench:    {} ratio (+/- 0)",
        group_name,
        bench_name,
        (compressed_size as f64) / (uncompressed_size as f64),
    );

    println!(
        "test {} Compressed Size/{} ... bench:    {} bytes (+/- 0)",
        group_name, bench_name, compressed_size
    );

    println!("{}{}", uncompressed_tree, compressed_tree);
}

fn yellow_taxi_trip_data(c: &mut Criterion) {
    taxi_data_parquet();
    let group_name = "Yellow Taxi Trip Data";
    let mut group = c.benchmark_group(format!("{} Compression Time", group_name));
    group.sample_size(10);
    benchmark_compress(
        &SamplingCompressor::default(),
        fetch_taxi_data,
        group_name,
        &mut group,
        "compress",
    );
    group.finish()
}

fn public_bi_benchmark(c: &mut Criterion) {
    let group_name = "Public BI";
    let mut group = c.benchmark_group(format!("{} Compression Time", group_name));
    group.sample_size(10);
    // group.measurement_time(Duration::new(10, 0));

    for dataset_handle in [
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
        let dataset = BenchmarkDatasets::PBI(dataset_handle);

        benchmark_compress(
            &SamplingCompressor::default(),
            || dataset.to_vortex_array().unwrap(),
            group_name,
            &mut group,
            dataset_handle.dataset_name(),
        );
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
