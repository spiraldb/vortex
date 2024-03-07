use std::fs::{create_dir_all, File};
use std::path::Path;

use bench_vortex::compress_taxi_data;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

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

fn enc_compress(c: &mut Criterion) {
    download_taxi_data();

    c.bench_function("compress", |b| b.iter(|| black_box(compress_taxi_data())));
}

criterion_group!(benches, enc_compress);
criterion_main!(benches);
