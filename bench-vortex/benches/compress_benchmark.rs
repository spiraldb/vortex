use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::Medicare1;
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::{compress_taxi_data, tpch};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use vortex_sampling_compressor::compressors::fsst::FSSTCompressor;
use vortex_sampling_compressor::SamplingCompressor;

fn vortex_compress_taxi(c: &mut Criterion) {
    taxi_data_parquet();
    let mut group = c.benchmark_group("end to end - taxi");
    group.sample_size(10);
    group.bench_function("compress", |b| b.iter(|| black_box(compress_taxi_data())));
    group.finish()
}

fn vortex_compress_medicare1(c: &mut Criterion) {
    let dataset = BenchmarkDatasets::PBI(Medicare1);
    dataset.as_uncompressed();
    let mut group = c.benchmark_group("end to end - medicare");
    group.sample_size(10);
    group.bench_function("compress", |b| {
        b.iter(|| black_box(dataset.compress_to_vortex()))
    });
    group.finish()
}

fn vortex_compress_tpch(c: &mut Criterion) {
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let orders_vortex = rt.block_on(tpch::load_table(data_dir, "orders", &tpch::schema::ORDERS));

    let compressor = SamplingCompressor::default().excluding(&FSSTCompressor);
    let compressed = compressor.compress(&orders_vortex, None).unwrap();
    let ratio = (orders_vortex.nbytes() as f64) / (compressed.nbytes() as f64);
    println!("compression ratio: {ratio}");

    let mut group = c.benchmark_group("tpch");
    group.sample_size(10);
    group.bench_function("orders", |b| {
        b.iter(|| {
            std::hint::black_box(compressor.compress(std::hint::black_box(&orders_vortex), None))
        });
    });

    let compressor_fsst = SamplingCompressor::default();

    group.bench_function("orders-fsst", |b| {
        b.iter(|| {
            std::hint::black_box(
                compressor_fsst.compress(std::hint::black_box(&orders_vortex), None),
            )
        });
    });

    let compressed = compressor_fsst.compress(&orders_vortex, None).unwrap();
    let ratio = (orders_vortex.nbytes() as f64) / (compressed.nbytes() as f64);
    println!("compression ratio: {ratio}");
}

criterion_group!(
    benches,
    vortex_compress_taxi,
    vortex_compress_medicare1,
    vortex_compress_tpch
);
criterion_main!(benches);
