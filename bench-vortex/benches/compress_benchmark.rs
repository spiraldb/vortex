use bench_vortex::medicare_data::medicare_data_csv;
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::{compress_medicare_data, compress_taxi_data};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn vortex_compress_taxi(c: &mut Criterion) {
    taxi_data_parquet();
    let mut group = c.benchmark_group("end to end");
    group.sample_size(10);
    group.bench_function("compress", |b| b.iter(|| black_box(compress_taxi_data())));
    group.finish()
}

fn vortex_compress_medicare(c: &mut Criterion) {
    medicare_data_csv();
    let mut group = c.benchmark_group("end to end");
    group.sample_size(10);
    group.bench_function("compress", |b| {
        b.iter(|| black_box(compress_medicare_data()))
    });
    group.finish()
}

criterion_group!(benches, vortex_compress_taxi, vortex_compress_medicare);
criterion_main!(benches);
