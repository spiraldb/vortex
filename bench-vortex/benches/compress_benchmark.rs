use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::Medicare1;
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::{compress_taxi_data, tpch};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use vortex::{IntoArray, IntoCanonical};
// use vortex_sampling_compressor::compressors::fsst::FSSTCompressor;
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
    let lineitem_vortex = rt.block_on(tpch::load_table(
        data_dir,
        "lineitem",
        &tpch::schema::LINEITEM,
    ));

    // Find the size of the comments column
    // let comments_nbytes = lineitem_vortex
    //     .with_dyn(|a| a.as_struct_array_unchecked().field_by_name("l_comment"))
    //     .unwrap()
    //     .nbytes();
    // println!("l_comment has size {}B", comments_nbytes);

    // let compressor = SamplingCompressor::default().excluding(&FSSTCompressor);
    // let compressed = compressor.compress(&lineitem_vortex, None).unwrap();
    // let ratio = (lineitem_vortex.nbytes() as f64) / (compressed.nbytes() as f64);
    // println!("compression ratio (without FSST): {ratio}");

    //
    // Full LINEITEM table from TPC-H
    //
    // let mut group = c.benchmark_group("tpch");
    // group.sample_size(10);
    // group.bench_function("lineitem", |b| {
    //     b.iter(|| {
    //         std::hint::black_box(compressor.compress(std::hint::black_box(&lineitem_vortex), None))
    //     });
    // });

    let compressor_fsst = SamplingCompressor::default();

    // group.bench_function("lineitem-fsst", |b| {
    //     b.iter(|| {
    //         std::hint::black_box(
    //             compressor_fsst.compress(std::hint::black_box(&lineitem_vortex), None),
    //         )
    //     });
    // });
    // group.finish();

    // let compressed = compressor_fsst.compress(&lineitem_vortex, None).unwrap();
    // let ratio = (lineitem_vortex.nbytes() as f64) / (compressed.nbytes() as f64);
    // println!("compression ratio (with FSST): {ratio}");

    //
    // LINEITEM table l_comment column only
    //
    let mut group = c.benchmark_group("l_comment");
    let comments = lineitem_vortex.with_dyn(|a| {
        a.as_struct_array_unchecked()
            .field_by_name("l_comment")
            .unwrap()
    });

    // println!(
    //     "running l_comment benchmark over array of size {}B",
    //     comments.nbytes()
    // );

    group.sample_size(10);
    // group.bench_function("compress-default", |b| {
    //     b.iter_with_large_drop(|| {
    //         std::hint::black_box(compressor.compress(&comments, None)).unwrap()
    //     });
    // });

    // group.bench_function("compress-fsst-chunked", |b| {
    //     b.iter_with_large_drop(|| {
    //         std::hint::black_box(compressor_fsst.compress(&comments, None)).unwrap()
    //     });
    // });

    // println!(
    //     "chunked compressed encoding: {}",
    //     compressor_fsst
    //         .compress(&comments, None)
    //         .unwrap()
    //         .array()
    //         .tree_display()
    // );

    let comments_canonical = comments
        .into_canonical()
        .unwrap()
        .into_varbin()
        .unwrap()
        .into_array();
    println!(
        "comments_canonical.nbytes() = {}B",
        comments_canonical.nbytes()
    );
    group.bench_function("compress-fsst-canonicalized", |b| {
        b.iter_with_large_drop(|| {
            std::hint::black_box(compressor_fsst.compress(&comments_canonical, None)).unwrap()
        });
    });

    println!(
        "canonical compressed encoding: {}",
        compressor_fsst
            .compress(&comments_canonical, None)
            .unwrap()
            .array()
            .tree_display()
    );

    group.finish();
}

criterion_group!(
    benches,
    vortex_compress_taxi,
    vortex_compress_medicare1,
    vortex_compress_tpch
);
criterion_main!(benches);
