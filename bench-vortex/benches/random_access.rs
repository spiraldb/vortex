use bench_vortex::reader::{take_parquet, take_vortex_object_store, take_vortex_tokio};
use bench_vortex::taxi_data::{taxi_data_parquet, taxi_data_vortex};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mimalloc::MiMalloc;
use tokio::runtime::Runtime;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("random access");

    let indices = [10, 11, 12, 13, 100_000, 3_000_000];

    let taxi_vortex = taxi_data_vortex();
    group.bench_function("vortex tokio", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| async { black_box(take_vortex_tokio(&taxi_vortex, &indices).await.unwrap()) })
    });

    group.bench_function("vortex object_store", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| async {
            black_box(
                take_vortex_object_store(&taxi_vortex, &indices)
                    .await
                    .unwrap(),
            )
        })
    });

    let taxi_parquet = taxi_data_parquet();
    group.sample_size(10).bench_function("parquet", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| async { black_box(take_parquet(&taxi_parquet, &indices).await.unwrap()) })
    });
}

criterion_group!(benches, random_access);
criterion_main!(benches);
