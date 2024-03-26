use bench_vortex::taxi_data::{
    download_taxi_data, take_taxi_data, take_taxi_data_arrow, write_taxi_data,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("random access");
    group.sample_size(10);

    let indices = [10, 11, 12, 13, 100_000, 3_000_000];

    let taxi_vortex = write_taxi_data();
    group.bench_function("vortex", |b| {
        b.iter(|| black_box(take_taxi_data(&taxi_vortex, &indices)))
    });

    let taxi_parquet = download_taxi_data();
    group.bench_function("arrow", |b| {
        b.iter(|| black_box(take_taxi_data_arrow(&taxi_parquet, &indices)))
    });
}

criterion_group!(benches, random_access);
criterion_main!(benches);
