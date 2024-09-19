use std::path::Path;
use std::time::Duration;

use arrow_array::RecordBatch;
use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::*;
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::{fetch_taxi_data, tpch};
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use vortex::array::{ChunkedArray, StructArray};
use vortex::{Array, IntoArray, IntoCanonical};
use vortex_dtype::field::Field;
use vortex_sampling_compressor::compressors::fsst::FSSTCompressor;
use vortex_sampling_compressor::SamplingCompressor;
use vortex_serde::layouts::LayoutWriter;

fn parquet_written_size(array: &Array, filepath: &str, compression: Compression) -> usize {
    let mut file = std::fs::File::create(Path::new(filepath)).unwrap();
    let chunked = ChunkedArray::try_from(array).unwrap();
    let chunks_vec = chunked.chunks().collect::<Vec<_>>();

    if chunks_vec.is_empty() {
        panic!("empty chunks");
    }

    let schema = RecordBatch::try_from(chunks_vec[0].clone())
        .unwrap()
        .schema();

    let writer_properties = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut writer = ArrowWriter::try_new(&mut file, schema, Some(writer_properties)).unwrap();
    for chunk in chunks_vec {
        let record_batch = RecordBatch::try_from(chunk).unwrap();
        writer.write(&record_batch).unwrap();
    }
    let n_bytes = writer.bytes_written();
    writer.close().unwrap();
    n_bytes
}

fn vortex_written_size(array: &Array, filepath: &str) -> u64 {
    async fn run(array: &Array, filepath: &str) -> u64 {
        let file = File::create(Path::new(filepath)).await.unwrap();
        let mut writer = LayoutWriter::new(file);

        writer = writer.write_array_columns(array.clone()).await.unwrap();
        writer
            .finalize()
            .await
            .unwrap()
            .stream_position()
            .await
            .unwrap()
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(array, filepath))
}

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
    let uncompressed = make_uncompressed();
    let uncompressed_size = uncompressed.as_ref().nbytes();
    // let mut uncompressed_tree: String = "".to_string();
    let mut compressed_size = 0;
    // let mut compressed_tree: String = "".to_string();

    group.throughput(Throughput::Bytes(uncompressed_size as u64));
    group.bench_function(format!("{} compressed throughput", bench_name), |b| {
        b.iter_with_large_drop(|| {
            let compressed = black_box(compressor.compress(uncompressed.as_ref(), None)).unwrap();
            compressed_size = compressed.nbytes();
            // uncompressed_tree = format!("{}", uncompressed.as_ref().tree_display());
            // compressed_tree = format!("{}", compressed.as_ref().tree_display());
        });
    });

    let vortex_nbytes = vortex_written_size(
        &compressor
            .compress(uncompressed.as_ref(), None)
            .unwrap()
            .into_array(),
        &format!("{}-{}.vortex", group_name, bench_name),
    );

    let parquet_zstd_nbytes = parquet_written_size(
        uncompressed.as_ref(),
        &format!("{}-{}.zstd.parquet", group_name, bench_name),
        Compression::ZSTD(ZstdLevel::default()),
    );

    let parquet_uncompressed_nbytes = parquet_written_size(
        uncompressed.as_ref(),
        &format!("{}-{}.uncompressed.parquet", group_name, bench_name),
        Compression::UNCOMPRESSED,
    );

    println!(
        "test {} Vortex-to-ParquetZstd Ratio/{} ... bench:    {} ratio (+/- 0)",
        group_name,
        bench_name,
        (vortex_nbytes as f64) / (parquet_zstd_nbytes as f64)
    );

    println!(
        "test {} Vortex-to-ParquetUncompressed Ratio/{} ... bench:    {} ratio (+/- 0)",
        group_name,
        bench_name,
        (vortex_nbytes as f64) / (parquet_uncompressed_nbytes as f64)
    );

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

    // println!("{}{}", uncompressed_tree, compressed_tree);
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
        "taxi",
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
        // Food,
        // HashTags,
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
    group.measurement_time(Duration::new(15, 0));

    let comments = StructArray::try_from(lineitem_vortex)
        .unwrap()
        .project(&[Field::Name("l_comment".to_string())])
        .unwrap()
        .into_array();
    println!("{}", comments.tree_display());

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
