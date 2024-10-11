use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema;
use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::*;
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::{fetch_taxi_data, tpch};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use futures::TryStreamExt;
use lazy_static::lazy_static;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use regex::Regex;
use tokio::runtime::Runtime;
use vortex::array::{ChunkedArray, StructArray};
use vortex::encoding::EncodingRef;
use vortex::{Array, ArrayDType, Context, IntoArray, IntoCanonical};
use vortex_alp::{ALPEncoding, ALPRDEncoding};
use vortex_bytebool::ByteBoolEncoding;
use vortex_datetime_parts::DateTimePartsEncoding;
use vortex_dict::DictEncoding;
use vortex_dtype::field::Field;
use vortex_error::VortexResult;
use vortex_fastlanes::{BitPackedEncoding, DeltaEncoding, FoREncoding};
use vortex_fsst::FSSTEncoding;
use vortex_roaring::{RoaringBoolEncoding, RoaringIntEncoding};
use vortex_runend::RunEndEncoding;
use vortex_runend_bool::RunEndBoolEncoding;
use vortex_sampling_compressor::compressors::fsst::FSSTCompressor;
use vortex_sampling_compressor::SamplingCompressor;
use vortex_serde::layouts::{LayoutContext, LayoutDeserializer, LayoutReaderBuilder, LayoutWriter};
use vortex_zigzag::ZigZagEncoding;

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

fn chunked_to_vec_record_batch(chunked: ChunkedArray) -> (Vec<RecordBatch>, Arc<Schema>) {
    let chunks_vec = chunked.chunks().collect::<Vec<_>>();

    if chunks_vec.is_empty() {
        panic!("empty chunks");
    }

    let batches = chunks_vec
        .iter()
        .map(|x| RecordBatch::try_from(x.clone()).unwrap())
        .collect::<Vec<_>>();
    let schema = batches[0].schema();
    (batches, schema)
}

fn parquet_compress_write(
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
    compression: Compression,
) -> usize {
    parquet_compress_write_buffer(batches, schema, compression, &mut Vec::new())
}

fn parquet_compress_write_buffer(
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
    compression: Compression,
    buf: &mut Vec<u8>,
) -> usize {
    let mut buf = Cursor::new(buf);
    let writer_properties = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(writer_properties)).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.flush().unwrap();
    let n_bytes = writer.bytes_written();
    writer.close().unwrap();
    n_bytes
}

fn parquet_decompress_from(buf: bytes::Bytes) -> usize {
    let builder = ParquetRecordBatchReaderBuilder::try_new(buf).unwrap();
    let reader = builder.build().unwrap();
    let mut nbytes = 0;
    for batch in reader {
        nbytes += batch.unwrap().get_array_memory_size()
    }
    nbytes
}

fn parquet_written_size(array: &Array, compression: Compression) -> usize {
    let chunked = ChunkedArray::try_from(array).unwrap();
    let (batches, schema) = chunked_to_vec_record_batch(chunked);
    parquet_compress_write(batches, schema, compression)
}

fn vortex_compress_write(
    runtime: &Runtime,
    compressor: &SamplingCompressor<'_>,
    array: &Array,
    buf: &mut Vec<u8>,
) -> VortexResult<u64> {
    async fn async_write(array: &Array, cursor: &mut Cursor<&mut Vec<u8>>) -> VortexResult<()> {
        let mut writer = LayoutWriter::new(cursor);

        writer = writer.write_array_columns(array.clone()).await?;
        writer.finalize().await?;
        Ok(())
    }

    let compressed = compressor.compress(array, None)?.into_array();
    // println!("compressed: {}", compressed.tree_display());
    let mut cursor = Cursor::new(buf);

    runtime.block_on(async_write(&compressed, &mut cursor))?;

    Ok(cursor.position())
}

fn vortex_compressed_written_size(
    runtime: &Runtime,
    compressor: &SamplingCompressor<'_>,
    array: &Array,
) -> VortexResult<u64> {
    vortex_compress_write(runtime, compressor, array, &mut Vec::new())
}

lazy_static! {
    pub static ref MAXIMAL_CTX: Arc<Context> = Arc::new(Context::default().with_encodings([
        &ALPEncoding as EncodingRef,
        &ByteBoolEncoding,
        &DateTimePartsEncoding,
        &DictEncoding,
        &BitPackedEncoding,
        &DeltaEncoding,
        &FoREncoding,
        &FSSTEncoding,
        &RoaringBoolEncoding,
        &RoaringIntEncoding,
        &RunEndEncoding,
        &RunEndBoolEncoding,
        &ZigZagEncoding,
        &ALPRDEncoding,
    ]));
}

fn vortex_decompress_read(runtime: &Runtime, buf: Arc<Vec<u8>>) -> VortexResult<ArrayRef> {
    async fn async_read(buf: Arc<Vec<u8>>) -> VortexResult<Array> {
        let builder: LayoutReaderBuilder<_> = LayoutReaderBuilder::new(
            buf,
            LayoutDeserializer::new(MAXIMAL_CTX.clone(), LayoutContext::default().into()),
        );

        let stream = builder.build().await?;
        let dtype = stream.schema().clone().into();
        let vecs: Vec<Array> = stream.try_collect().await?;

        let a: Array = ChunkedArray::try_new(vecs, dtype).map(|e| e.into())?;
        // println!("{}", a.tree_display());
        Ok(a)
    }

    runtime
        .block_on(async_read(buf))?
        .into_canonical()?
        .into_arrow()
}

fn benchmark_compress<F, U>(
    c: &mut Criterion,
    compressor: &SamplingCompressor<'_>,
    make_uncompressed: F,
    sample_size: usize,
    measurement_time: Option<Duration>,
    bench_name: &str,
) where
    F: Fn() -> U,
    U: AsRef<Array>,
{
    ensure_dir_exists("benchmarked-files").unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let uncompressed = make_uncompressed();
    let uncompressed_size = uncompressed.as_ref().nbytes();
    let mut compressed_size = 0;

    {
        let mut group = c.benchmark_group("compress time");
        group.sample_size(sample_size);
        group.throughput(Throughput::Bytes(uncompressed_size as u64));
        measurement_time.map(|t| group.measurement_time(t));
        group.bench_function(bench_name, |b| {
            b.iter_with_large_drop(|| {
                compressed_size = black_box(
                    vortex_compress_write(
                        &runtime,
                        compressor,
                        uncompressed.as_ref(),
                        &mut Vec::new(),
                    )
                    .unwrap(),
                );
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("parquet_rs-zstd compress time");
        group.sample_size(sample_size);
        group.throughput(Throughput::Bytes(uncompressed_size as u64));
        measurement_time.map(|t| group.measurement_time(t));
        group.bench_function(bench_name, |b| {
            let chunked = ChunkedArray::try_from(uncompressed.as_ref()).unwrap();
            let (batches, schema) = chunked_to_vec_record_batch(chunked);
            b.iter_with_large_drop(|| {
                black_box(parquet_compress_write(
                    batches.clone(),
                    schema.clone(),
                    Compression::ZSTD(ZstdLevel::default()),
                ));
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("decompress time");
        group.sample_size(sample_size);
        // FIXME(DK): decompress from a "file"
        group.throughput(Throughput::Bytes(uncompressed_size as u64));
        measurement_time.map(|t| group.measurement_time(t));
        group.bench_function(bench_name, |b| {
            let mut buf = Vec::new();
            vortex_compress_write(&runtime, compressor, uncompressed.as_ref(), &mut buf).unwrap();
            let arc = Arc::new(buf);
            // println!("{}", uncompressed.as_ref().tree_display());
            b.iter_with_large_drop(|| {
                black_box(vortex_decompress_read(&runtime, arc.clone()).unwrap());
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("parquet_rs-zstd decompress time");
        group.sample_size(sample_size);
        group.throughput(Throughput::Bytes(uncompressed_size as u64));
        measurement_time.map(|t| group.measurement_time(t));
        group.bench_function(bench_name, |b| {
            let chunked = ChunkedArray::try_from(uncompressed.as_ref()).unwrap();
            let (batches, schema) = chunked_to_vec_record_batch(chunked);
            let mut buf = Vec::new();
            parquet_compress_write_buffer(
                batches.clone(),
                schema.clone(),
                Compression::ZSTD(ZstdLevel::default()),
                &mut buf,
            );
            let bytes = bytes::Bytes::from(buf);
            b.iter_with_large_drop(|| {
                black_box(parquet_decompress_from(bytes.clone()));
            });
        });
        group.finish();
    }

    if env::var("BENCH_VORTEX_RATIOS")
        .ok()
        .map(|x| Regex::new(&x).unwrap().is_match(bench_name))
        .unwrap_or(false)
    {
        let vortex_nbytes =
            vortex_compressed_written_size(&runtime, compressor, uncompressed.as_ref()).unwrap();

        let parquet_zstd_nbytes = parquet_written_size(
            uncompressed.as_ref(),
            Compression::ZSTD(ZstdLevel::default()),
        );

        println!(
            "{}",
            serde_json::to_string(&GenericBenchmarkResults {
                name: &format!("vortex:parquet-zstd size/{}", bench_name),
                value: (vortex_nbytes as f64) / (parquet_zstd_nbytes as f64),
                unit: "ratio",
                range: 0.0,
            })
            .unwrap()
        );

        println!(
            "{}",
            serde_json::to_string(&GenericBenchmarkResults {
                name: &format!("vortex:raw size/{}", bench_name),
                value: (compressed_size as f64) / (uncompressed_size as f64),
                unit: "ratio",
                range: 0.0,
            })
            .unwrap()
        );

        println!(
            "{}",
            serde_json::to_string(&GenericBenchmarkResults {
                name: &format!("vortex size/{}", bench_name),
                value: compressed_size as f64,
                unit: "bytes",
                range: 0.0,
            })
            .unwrap()
        );
    }
}

fn yellow_taxi_trip_data(c: &mut Criterion) {
    taxi_data_parquet();
    benchmark_compress(
        c,
        &SamplingCompressor::default(),
        fetch_taxi_data,
        10,
        None,
        "taxi",
    );
}

fn public_bi_benchmark(c: &mut Criterion) {
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
            c,
            &SamplingCompressor::default(),
            || dataset.to_vortex_array().unwrap(),
            10,
            None,
            dataset_handle.dataset_name(),
        );
    }
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
        c,
        &compressor,
        || &comments,
        10,
        None,
        "TPC-H l_comment chunked without fsst",
    );

    benchmark_compress(
        c,
        &compressor_fsst,
        || &comments,
        10,
        None,
        "TPC-H l_comment chunked",
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
        c,
        &compressor_fsst,
        || &comments_canonical_chunked,
        10,
        Some(Duration::new(15, 0)),
        "TPC-H l_comment canonical",
    );
}

criterion_group!(
    benches,
    yellow_taxi_trip_data,
    public_bi_benchmark,
    tpc_h_l_comment,
);
criterion_main!(benches);
