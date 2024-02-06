use std::fs::File;
use std::path::Path;

use arrow::array::RecordBatchReader;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use enc::array::chunked::ChunkedArray;
use enc::array::typed::TypedArray;
use enc::array::{Array, ArrayRef};
use enc::compress::CompressCtx;
use enc::dtype::DType;
use enc::error::{EncError, EncResult};
use enc::stats::Stat;

fn download_taxi_data() -> &'static Path {
    let download_path = Path::new("../../pyspiral/bench/.data/https-d37ci6vzurychx-cloudfront-net-trip-data-yellow-tripdata-2023-11.parquet");
    if download_path.exists() {
        return download_path;
    }

    let mut download_file = File::create(download_path).unwrap();
    reqwest::blocking::get(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet",
    )
    .unwrap()
    .copy_to(&mut download_file)
    .unwrap();

    download_path
}

fn compress(array: ArrayRef) -> usize {
    let compressed = CompressCtx::default().compress(array.as_ref());
    println!("NBytes {}", compressed.nbytes());
    println!(
        "Ratio {}",
        compressed.nbytes() as f32 / array.nbytes() as f32
    );
    compressed.nbytes()
}

fn enc_compress(c: &mut Criterion) {
    let file = File::open(download_taxi_data()).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(128_000)
        .build()
        .unwrap();

    let schema = reader.schema();
    let dtype: DType = schema.try_into().unwrap();
    let chunks = reader
        .map(|batch_result| batch_result.map_err(EncError::from))
        .map(|batch| batch.map(|b| b.into()))
        .collect::<EncResult<Vec<ArrayRef>>>()
        .unwrap()
        .iter()
        // FIXME(ngates): we shouldn't have to do this ourselves...
        .map(|chunk| TypedArray::maybe_wrap(chunk.clone(), &dtype))
        .collect();
    let chunked = ChunkedArray::new(chunks, dtype);
    chunked.chunks().iter().for_each(|a| {
        if matches!(a.dtype(), DType::Int(_, _, _) | DType::Float(_, _)) {
            black_box(a.stats().get_or_compute(&Stat::Min));
        }
    });
    println!(
        "{} rows in {} chunks",
        chunked.len(),
        chunked.chunks().len()
    );
    let array = chunked.boxed();

    c.bench_function("enc.compress", |b| {
        b.iter(|| compress(black_box(array.clone())))
    });
}

criterion_group!(benches, enc_compress);
criterion_main!(benches);
