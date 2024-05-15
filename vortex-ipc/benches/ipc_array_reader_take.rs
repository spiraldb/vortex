use std::io::Cursor;

use criterion::async_executor::FuturesExecutor;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures_util::{pin_mut, TryStreamExt};
use itertools::Itertools;
use vortex::array::primitive::PrimitiveArray;
use vortex::{Context, IntoArray};
use vortex_dtype::Nullability;
use vortex_dtype::{DType, PType};
use vortex_ipc::array_stream::ArrayStreamExt;
use vortex_ipc::io::FuturesVortexRead;
use vortex_ipc::writer::StreamWriter;
use vortex_ipc::MessageReader;

// 100 record batches, 100k rows each
// take from the first 20 batches and last batch
// compare with arrow
fn ipc_array_reader_take(c: &mut Criterion) {
    let ctx = Context::default();

    let indices = (0..20)
        .map(|i| i * 100_000 + 1)
        .chain([98 * 100_000 + 1])
        .collect_vec();
    let mut group = c.benchmark_group("ipc_array_reader_take");

    group.bench_function("vortex", |b| {
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            let mut writer = StreamWriter::try_new(&mut cursor, &ctx).unwrap();
            writer
                .write_schema(&DType::Primitive(PType::I32, Nullability::Nullable))
                .unwrap();
            (0..100i32).for_each(|i| {
                let data = PrimitiveArray::from(vec![i; 100_000]).into_array();
                writer.write_batch(&data).unwrap();
            });
        }
        let indices = indices.clone().into_array();

        b.to_async(FuturesExecutor).iter(|| async {
            let mut cursor = futures_util::io::Cursor::new(&buffer);
            let mut msgs = MessageReader::try_new(FuturesVortexRead(&mut cursor))
                .await
                .unwrap();
            let stream = msgs
                .array_stream_from_messages(&ctx)
                .await
                .unwrap()
                .take_rows(&indices)
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
