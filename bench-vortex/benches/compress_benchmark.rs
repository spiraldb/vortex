use bench_vortex::compress_taxi_data;
use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::Medicare1;
use bench_vortex::taxi_data::taxi_data_parquet;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn vortex_compress_taxi(c: &mut Criterion) {
    taxi_data_parquet();
    let mut group = c.benchmark_group("end to end");
    group.sample_size(10);
    group.bench_function("compress", |b| b.iter(|| black_box(compress_taxi_data())));
    group.finish()
}

fn vortex_compress_medicare1(c: &mut Criterion) {
    let dataset = BenchmarkDatasets::PBI(Medicare1);
    dataset.as_uncompressed();
    let mut group = c.benchmark_group("end to end");
    group.sample_size(10);
    group.bench_function("compress", |b| {
        b.iter(|| black_box(dataset.compress_to_vortex()))
    });
    group.finish()
}

criterion_group!(benches, vortex_compress_taxi, vortex_compress_medicare1);
criterion_main!(benches);
