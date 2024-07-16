use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::tpch::{load_datasets, Format};
use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Builder;

fn benchmark(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    // Run TPC-H data gen.
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();

    let vortex_no_pushdown_ctx = runtime
        .block_on(load_datasets(
            &data_dir,
            Format::Vortex {
                disable_pushdown: false,
            },
        ))
        .unwrap();
    let vortex_ctx = runtime
        .block_on(load_datasets(
            &data_dir,
            Format::Vortex {
                disable_pushdown: true,
            },
        ))
        .unwrap();
    let csv_ctx = runtime
        .block_on(load_datasets(&data_dir, Format::Csv))
        .unwrap();
    let arrow_ctx = runtime
        .block_on(load_datasets(&data_dir, Format::Arrow))
        .unwrap();

    for q in 1..=22 {
        if q == 15 {
            // DataFusion does not support query 15 since it has multiple SQL statements.
        }

        let query = bench_vortex::tpch::tpch_query(q);

        let mut group = c.benchmark_group(format!("tpch_q{q}"));
        group.sample_size(10);

        group.bench_function("vortex-pushdown", |b| {
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

        group.bench_function("vortex-nopushdown", |b| {
            b.to_async(&runtime).iter(|| async {
                vortex_no_pushdown_ctx
                    .sql(&query)
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap()
            })
        });

        group.bench_function("csv", |b| {
            b.to_async(&runtime)
                .iter(|| async { csv_ctx.sql(&query).await.unwrap().collect().await.unwrap() })
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
    }
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
