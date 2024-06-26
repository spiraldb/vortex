use std::sync::Arc;

use arrow_array::builder::{StringBuilder, UInt32Builder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion::datasource::MemTable;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::lit;
use datafusion::prelude::{col, SessionContext};
use lazy_static::lazy_static;
use vortex::compress::Compressor;
use vortex::encoding::EncodingRef;
use vortex::{Array, Context, IntoArray, ToArrayData};
use vortex_datafusion::{VortexMemTable, VortexMemTableOptions};
use vortex_fastlanes::{BitPackedEncoding, DeltaEncoding, FoREncoding};

lazy_static! {
    pub static ref CTX: Context = Context::default().with_encodings([
        &BitPackedEncoding as EncodingRef,
        // &DictEncoding,
        &FoREncoding,
        &DeltaEncoding,
    ]);
}

fn toy_dataset_arrow() -> RecordBatch {
    // 64,000 rows of string and numeric data.
    // 8,000 values of first string, second string, third string, etc.

    let names = vec![
        "Alexander",
        "Anastasia",
        "Archibald",
        "Bartholomew",
        "Benjamin",
        "Christopher",
        "Elizabeth",
        "Gabriella",
    ];

    let mut col1 = StringBuilder::with_capacity(64_000, 64_000_000);
    let mut col2 = UInt32Builder::with_capacity(64_000);
    for i in 0..64_000 {
        col1.append_value(names[i % 8]);
        col2.append_value(u32::try_from(i).unwrap());
    }

    let col1 = col1.finish();
    let col2 = col2.finish();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("names", DataType::Utf8, false),
            Field::new("scores", DataType::UInt32, false),
        ])),
        vec![Arc::new(col1), Arc::new(col2)],
    )
    .unwrap()
}

fn toy_dataset_vortex() -> Array {
    let uncompressed = toy_dataset_arrow().to_array_data().into_array();
    println!("uncompressed vortex size: {}B", uncompressed.nbytes());

    let compressor = Compressor::new(&CTX);
    let compressed = compressor.compress(&uncompressed, None).unwrap();
    println!("compressed vortex size: {} B", compressed.nbytes());
    compressed
}

fn bench_datafusion(c: &mut Criterion) {
    let mut group = c.benchmark_group("datafusion");

    let session = SessionContext::new();

    let arrow_dataset = toy_dataset_arrow();
    let arrow_table =
        Arc::new(MemTable::try_new(arrow_dataset.schema(), vec![vec![arrow_dataset]]).unwrap());

    group.bench_function("arrow", |b| {
        let arrow_table = arrow_table.clone();
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        )
        .iter(|| async {
            black_box(session.read_table(arrow_table.clone()).unwrap())
                .filter(col("scores").gt_eq(lit(3_000)))
                .unwrap()
                .filter(col("scores").lt_eq(lit(4_000)))
                .unwrap()
                .aggregate(vec![], vec![sum(col("scores"))])
                .unwrap()
                .collect()
                .await
                .unwrap();
        })
    });

    let vortex_dataset = toy_dataset_vortex();
    let vortex_table_pushdown = Arc::new(
        VortexMemTable::try_new(vortex_dataset, VortexMemTableOptions::default()).unwrap(),
    );
    group.bench_function("vortex_pushdown", |b| {
        let vortex_table_pushdown = vortex_table_pushdown.clone();
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                black_box(session.read_table(vortex_table_pushdown.clone()).unwrap())
                    .filter(col("scores").gt_eq(lit(3_000)))
                    .unwrap()
                    .filter(col("scores").lt_eq(lit(4_000)))
                    .unwrap()
                    .aggregate(vec![], vec![sum(col("scores"))])
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
            })
    });

    let vortex_dataset = toy_dataset_vortex();
    let vortex_table_no_pushdown = Arc::new(
        VortexMemTable::try_new(
            vortex_dataset,
            VortexMemTableOptions::default().with_disable_pushdown(true),
        )
        .unwrap(),
    );
    group.bench_function("vortex_no_pushdown", |b| {
        let vortex_table_no_pushdown = vortex_table_no_pushdown.clone();
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        )
        .iter(|| async {
            black_box(
                session
                    .read_table(vortex_table_no_pushdown.clone())
                    .unwrap(),
            )
            .filter(col("scores").gt_eq(lit(3_000)))
            .unwrap()
            .filter(col("scores").lt_eq(lit(4_000)))
            .unwrap()
            .aggregate(vec![], vec![sum(col("scores"))])
            .unwrap()
            .collect()
            .await
            .unwrap();
        })
    });
}

criterion_group!(benches, bench_datafusion);
criterion_main!(benches);
