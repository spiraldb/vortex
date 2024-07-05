use std::env;
use std::sync::Arc;

use bench_vortex::reader::{
    take_parquet, take_parquet_object_store, take_vortex_object_store, take_vortex_tokio,
};
use bench_vortex::taxi_data::{taxi_data_parquet, taxi_data_vortex};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mimalloc::MiMalloc;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use tokio::runtime::Runtime;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const INDICES: [u64; 6] = [10, 11, 12, 13, 100_000, 3_000_000];

fn random_access_vortex(c: &mut Criterion) {
    let mut group = c.benchmark_group("vortex");

    let taxi_vortex = taxi_data_vortex();
    group.bench_function("tokio local disk", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| async { black_box(take_vortex_tokio(&taxi_vortex, &INDICES).await.unwrap()) })
    });

    let local_fs = LocalFileSystem::new();
    let local_fs_path = object_store::path::Path::from_filesystem_path(&taxi_vortex).unwrap();
    group.bench_function("localfs", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| async {
            black_box(
                take_vortex_object_store(&local_fs, &local_fs_path, &INDICES)
                    .await
                    .unwrap(),
            )
        })
    });

    let r2_fs = AmazonS3Builder::new()
        .with_access_key_id(env::var("ACCESS_KEY_ID").unwrap())
        .with_secret_access_key(env::var("SECRET_ACCESS_KEY").unwrap())
        .with_endpoint("https://01e9655179bbec953276890b183039bc.r2.cloudflarestorage.com")
        .with_bucket_name("vortex-test")
        .build()
        .unwrap();
    let r2_path =
        object_store::path::Path::from_url_path(taxi_vortex.file_name().unwrap().to_str().unwrap())
            .unwrap();
    group.sample_size(10).bench_function("R2", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| async {
            black_box(
                take_vortex_object_store(&r2_fs, &r2_path, &INDICES)
                    .await
                    .unwrap(),
            )
        })
    });
}

fn random_access_parquet(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet");
    group.sample_size(10);

    let r2_fs = Arc::new(
        AmazonS3Builder::new()
            .with_access_key_id(env::var("ACCESS_KEY_ID").unwrap())
            .with_secret_access_key(env::var("SECRET_ACCESS_KEY").unwrap())
            .with_endpoint("https://01e9655179bbec953276890b183039bc.r2.cloudflarestorage.com")
            .with_bucket_name("vortex-test")
            .build()
            .unwrap(),
    );

    let taxi_parquet = taxi_data_parquet();
    group.bench_function("tokio local disk", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| async { black_box(take_parquet(&taxi_parquet, &INDICES).await.unwrap()) })
    });

    let r2_parquet_path = object_store::path::Path::from_url_path(
        taxi_parquet.file_name().unwrap().to_str().unwrap(),
    )
    .unwrap();
    group.bench_function("R2", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| async {
            black_box(
                take_parquet_object_store(r2_fs.clone(), &r2_parquet_path, &INDICES)
                    .await
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, random_access_vortex, random_access_parquet);
criterion_main!(benches);
