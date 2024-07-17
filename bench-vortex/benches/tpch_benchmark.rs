use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::tpch::{load_datasets, tpch_queries, Format};
use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Builder;

fn benchmark(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    // Run TPC-H data gen.
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();

    let vortex_pushdown_disabled_ctx = runtime
        .block_on(load_datasets(
            &data_dir,
            Format::Vortex {
                disable_pushdown: true,
            },
        ))
        .unwrap();
    let vortex_ctx = runtime
        .block_on(load_datasets(
            &data_dir,
            Format::Vortex {
                disable_pushdown: false,
            },
        ))
        .unwrap();
    let arrow_ctx = runtime
        .block_on(load_datasets(&data_dir, Format::Arrow))
        .unwrap();
    let parquet_ctx = runtime
        .block_on(load_datasets(&data_dir, Format::Parquet))
        .unwrap();

    for (q, query) in tpch_queries() {
        let mut group = c.benchmark_group(format!("tpch_q{q}"));
        group.sample_size(10);

        group.bench_function("vortex-pushdown-disabled", |b| {
            b.to_async(&runtime).iter(|| async {
                vortex_pushdown_disabled_ctx
                    .sql(&query)
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap()
            })
        });

        group.bench_function("vortex-pushdown-enabled", |b| {
            b.to_async(&runtime).iter(|| async {
                vortex_ctx
                    .sql(&query)
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap()
            })
        });

        group.bench_function("arrow", |b| {
            b.to_async(&runtime).iter(|| async {
                arrow_ctx
                    .sql(&query)
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap()
            })
        });

        group.bench_function("parquet", |b| {
            b.to_async(&runtime).iter(|| async {
                parquet_ctx
                    .sql(&query)
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap()
            })
        });
    }
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
