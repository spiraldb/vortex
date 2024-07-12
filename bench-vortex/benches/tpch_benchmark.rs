use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::tpch::query::Q1;
use bench_vortex::tpch::{load_datasets, Format};
use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Builder;

fn benchmark(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    // Run TPC-H data gen.
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();

    let mut group = c.benchmark_group("tpch q1");
    group.sample_size(10);

    let ctx = runtime
        .block_on(load_datasets(
            &data_dir,
            Format::Vortex {
                disable_pushdown: false,
            },
        ))
        .unwrap();
    group.bench_function("vortex-pushdown", |b| {
        b.to_async(&runtime)
            .iter(|| async { ctx.sql(Q1).await.unwrap().collect().await.unwrap() })
    });

    let ctx = runtime
        .block_on(load_datasets(
            &data_dir,
            Format::Vortex {
                disable_pushdown: true,
            },
        ))
        .unwrap();
    group.bench_function("vortex-nopushdown", |b| {
        b.to_async(&runtime)
            .iter(|| async { ctx.sql(Q1).await.unwrap().collect().await.unwrap() })
    });

    let ctx = runtime
        .block_on(load_datasets(&data_dir, Format::Csv))
        .unwrap();
    group.bench_function("csv", |b| {
        b.to_async(&runtime)
            .iter(|| async { ctx.sql(Q1).await.unwrap().collect().await.unwrap() })
    });

    let ctx = runtime
        .block_on(load_datasets(&data_dir, Format::Arrow))
        .unwrap();
    group.bench_function("arrow", |b| {
        b.to_async(&runtime)
            .iter(|| async { ctx.sql(Q1).await.unwrap().collect().await.unwrap() })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
