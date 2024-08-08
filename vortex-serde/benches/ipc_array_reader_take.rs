use std::sync::Arc;

use criterion::async_executor::FuturesExecutor;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures_executor::block_on;
use futures_util::{pin_mut, TryStreamExt};
use itertools::Itertools;
use vortex::array::{ChunkedArray, PrimitiveArray};
use vortex::stream::ArrayStreamExt;
use vortex::validity::Validity;
use vortex::{Context, IntoArray};
use vortex_serde::io::FuturesAdapter;
use vortex_serde::writer::ArrayWriter;
use vortex_serde::MessageReader;

// 100 record batches, 100k rows each
// take from the first 20 batches and last batch
// compare with arrow
fn ipc_array_reader_take(c: &mut Criterion) {
    let ctx = Arc::new(Context::default());

    let indices = (0..20)
        .map(|i| i * 100_000 + 1)
        .chain([98 * 100_000 + 1])
        .collect_vec();
    let mut group = c.benchmark_group("ipc_array_reader_take");

    group.bench_function("vortex", |b| {
        let array = ChunkedArray::from_iter(
            (0..100i32)
                .map(|i| vec![i; 100_000])
                .map(|vec| PrimitiveArray::from_vec(vec, Validity::AllValid).into_array()),
        )
        .into_array();

        let buffer = block_on(async { ArrayWriter::new(vec![]).write_array(array).await })
            .unwrap()
            .into_inner();

        let indices = indices.clone().into_array();

        b.to_async(FuturesExecutor).iter(|| async {
            let mut cursor = futures_util::io::Cursor::new(&buffer);
            let mut msgs = MessageReader::try_new(FuturesAdapter(&mut cursor))
                .await
                .unwrap();
            let stream = msgs
                .array_stream_from_messages(ctx.clone())
                .await
                .unwrap()
                .take_rows(indices.clone())
                .unwrap();
            pin_mut!(stream);

            while let Some(arr) = stream.try_next().await.unwrap() {
                black_box(arr);
            }
        });
    });
}

criterion_group!(benches, ipc_array_reader_take);
criterion_main!(benches);
