use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::path::Path;

use arrow_array::RecordBatchReader;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use vortex::array::ArrayRef;
use vortex::compress::{CompressConfig, CompressCtx};
use vortex_bench::enumerate_arrays;

fn download_taxi_data() -> &'static Path {
    let download_path = Path::new("data/yellow-tripdata-2023-11.parquet");
    if download_path.exists() {
        return download_path;
    }

    create_dir_all(download_path.parent().unwrap()).unwrap();
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
    CompressCtx::default()
        .compress(array.as_ref(), None)
        .unwrap()
        .nbytes()
}

fn enc_compress(c: &mut Criterion) {
    enumerate_arrays();

    let file = File::open(download_taxi_data()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mask = ProjectionMask::roots(builder.parquet_schema(), [6]);
    let mut reader = builder
        .with_projection(mask)
        .with_batch_size(200_000_000)
        //.with_limit(1_000_000)
        .build()
        .unwrap();

    let array = ArrayRef::try_from((&mut reader) as &mut dyn RecordBatchReader).unwrap();
    let cfg = CompressConfig::new(
        HashSet::from_iter(enumerate_arrays().iter().map(|e| (*e).id())),
        HashSet::default(),
    );
    println!("Compression config {cfg:?}");
    let compressed = CompressCtx::new(&cfg)
        .compress(array.as_ref(), None)
        .unwrap();
    println!("Compressed array {compressed}");
    println!(
        "NBytes {}, Ratio {}",
        compressed.nbytes(),
        compressed.nbytes() as f32 / array.nbytes() as f32
    );

    // black_box(compress(array.clone()));

    c.bench_function("compress", |b| {
        b.iter(|| black_box(compress(array.clone())))
    });
}

criterion_group!(benches, enc_compress);
criterion_main!(benches);
