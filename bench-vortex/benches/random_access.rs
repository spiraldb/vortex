use bench_vortex::data_downloads::{BenchmarkDataset, FileType};
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::Medicare1;
use bench_vortex::reader::{take_lance, take_parquet, take_vortex};
use bench_vortex::taxi_data::{taxi_data_lance, taxi_data_parquet, taxi_data_vortex};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("random access");
    group.sample_size(10);

    let indices = [10, 11, 12, 13, 100_000, 3_000_000];

    let taxi_vortex = taxi_data_vortex();
    group.bench_function("vortex", |b| {
        b.iter(|| black_box(take_vortex(&taxi_vortex, &indices).unwrap()))
    });

    let taxi_parquet = taxi_data_parquet();
    group.bench_function("arrow", |b| {
        b.iter(|| black_box(take_parquet(&taxi_parquet, &indices).unwrap()))
    });

    let taxi_lance = taxi_data_lance();
    group.bench_function("taxi_lance", |b| {
        b.iter(|| black_box(take_lance(&taxi_lance, &indices)))
    });

    let dataset = BenchmarkDatasets::PBI(Medicare1);
    dataset.write_as_parquet();
    dataset.write_as_lance();
    // NB: our parquet benchmarks read from a single file, and we (currently) write each
    // file to an individual lance dataset for comparison parity.
    // TODO(@jcasale): use datafusion for parquet random-access benchmarks and modify
    // lance-writing code to write a single dataset.
    let medicare_lance = dataset.list_files(FileType::Lance);
    group.bench_function("medic_lance2", |b| {
        b.iter(|| black_box(take_lance(medicare_lance.first().unwrap(), &indices)))
    });
}

criterion_group!(benches, random_access);
criterion_main!(benches);
