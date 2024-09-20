use std::fs;
use std::io::Cursor;
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
use vortex::array::{ChunkedArray, StructArray};
use vortex::{Array, ArrayDType, IntoArray, IntoCanonical};
use vortex_dtype::field::Field;
use vortex_sampling_compressor::compressors::fsst::FSSTCompressor;
use vortex_sampling_compressor::SamplingCompressor;
use vortex_serde::layouts::LayoutWriter;

#[derive(serde::Serialize)]
struct GenericBenchmarkResults<'a> {
    name: &'a str,
    value: f64,
    unit: &'a str,
    range: f64,
}

fn ensure_dir_exists(dir: &str) -> std::io::Result<()> {
    let path = Path::new(dir);
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

fn parquet_written_size(array: &Array, compression: Compression) -> usize {
    let mut buf = Cursor::new(Vec::new());
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
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(writer_properties)).unwrap();
    for chunk in chunks_vec {
        let record_batch = RecordBatch::try_from(chunk).unwrap();
        writer.write(&record_batch).unwrap();
    }
    writer.flush().unwrap();
    let n_bytes = writer.bytes_written();
    writer.close().unwrap();
    n_bytes
}

fn vortex_written_size(array: &Array) -> u64 {
    async fn run(array: &Array) -> u64 {
        let buf = Cursor::new(Vec::new());
        let mut writer = LayoutWriter::new(buf);

        writer = writer.write_array_columns(array.clone()).await.unwrap();
        let buf = writer.finalize().await.unwrap();
        buf.position()
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(array))
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
    ensure_dir_exists("benchmarked-files").unwrap();
    let uncompressed = make_uncompressed();
    let uncompressed_size = uncompressed.as_ref().nbytes();
    let mut compressed_size = 0;

    group.throughput(Throughput::Bytes(uncompressed_size as u64));
    group.bench_function(format!("{} compression", bench_name), |b| {
        b.iter_with_large_drop(|| {
            let compressed = black_box(compressor.compress(uncompressed.as_ref(), None)).unwrap();
            compressed_size = compressed.nbytes();
        });
    });

    let vortex_nbytes = vortex_written_size(
        &compressor
            .compress(uncompressed.as_ref(), None)
            .unwrap()
            .into_array(),
    );

    let parquet_zstd_nbytes = parquet_written_size(
        uncompressed.as_ref(),
        Compression::ZSTD(ZstdLevel::default()),
    );

    let parquet_uncompressed_nbytes =
        parquet_written_size(uncompressed.as_ref(), Compression::UNCOMPRESSED);

    println!(
        "{}",
        serde_json::to_string(&GenericBenchmarkResults {
            name: &format!("{} Vortex-to-ParquetZstd Ratio/{}", group_name, bench_name),
            value: (vortex_nbytes as f64) / (parquet_zstd_nbytes as f64),
            unit: "ratio",
            range: 0.0,
        })
        .unwrap()
    );

    println!(
        "{}",
        serde_json::to_string(&GenericBenchmarkResults {
            name: &format!(
                "{} Vortex-to-ParquetUncompressed Ratio/{}",
                group_name, bench_name
            ),
            value: (vortex_nbytes as f64) / (parquet_uncompressed_nbytes as f64),
            unit: "ratio",
            range: 0.0,
        })
        .unwrap()
    );

    println!(
        "{}",
        serde_json::to_string(&GenericBenchmarkResults {
            name: &format!("{} Compression Ratio/{}", group_name, bench_name),
            value: (compressed_size as f64) / (uncompressed_size as f64),
            unit: "ratio",
            range: 0.0,
        })
        .unwrap()
    );

    println!(
        "{}",
        serde_json::to_string(&GenericBenchmarkResults {
            name: &format!("{} Compression Size/{}", group_name, bench_name),
            value: compressed_size as f64,
            unit: "bytes",
            range: 0.0,
        })
        .unwrap()
    );
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
        Bimbo,
        CMSprovider,
        // Corporations, // duckdb thinks ' is a quote character but its used as an apostrophe
        // CityMaxCapita, // 11th column has F, M, and U but is inferred as boolean
        Euro2016,
        Food,
        HashTags,
        // Hatred, // panic in fsst_compress_iter
        // TableroSistemaPenal, // thread 'main' panicked at bench-vortex/benches/compress_benchmark.rs:224:42: called `Result::unwrap()` on an `Err` value: expected type: {column00=utf8?, column01=i64?, column02=utf8?, column03=f64?, column04=i64?, column05=utf8?, column06=utf8?, column07=utf8?, column08=utf8?, column09=utf8?, column10=i64?, column11=i64?, column12=utf8?, column13=utf8?, column14=i64?, column15=i64?, column16=utf8?, column17=utf8?, column18=utf8?, column19=utf8?, column20=i64?, column21=utf8?, column22=utf8?, column23=utf8?, column24=utf8?, column25=i64?, column26=utf8?} but instead got {column00=utf8?, column01=i64?, column02=i64?, column03=i64?, column04=i64?, column05=utf8?, column06=i64?, column07=i64?, column08=i64?, column09=utf8?, column10=ext(vortex.date, ExtMetadata([4]))?, column11=ext(vortex.date, ExtMetadata([4]))?, column12=utf8?, column13=utf8?, column14=utf8?, column15=i64?, column16=i64?, column17=utf8?, column18=utf8?, column19=utf8?, column20=utf8?, column21=utf8?}
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

    let comment_chunks = ChunkedArray::try_from(lineitem_vortex)
        .unwrap()
        .chunks()
        .map(|chunk| {
            StructArray::try_from(chunk)
                .unwrap()
                .project(&[Field::Name("l_comment".to_string())])
                .unwrap()
                .into_array()
        })
        .collect::<Vec<_>>();
    let comment_dtype = comment_chunks[0].dtype().clone();
    let comments = ChunkedArray::try_new(comment_chunks, comment_dtype)
        .unwrap()
        .into_array();

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
        .into_struct()
        .unwrap()
        .into_array();
    let dtype = comments_canonical.dtype().clone();
    let comments_canonical_chunked =
        ChunkedArray::try_new(vec![comments_canonical], dtype).unwrap();

    benchmark_compress(
        &compressor_fsst,
        || &comments_canonical_chunked,
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
