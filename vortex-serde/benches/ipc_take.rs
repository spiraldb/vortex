use std::sync::Arc;

use arrow::ipc::reader::StreamReader as ArrowStreamReader;
use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter as ArrowStreamWriter};
use arrow_ipc::{CompressionType, MetadataVersion};
use arrow_schema::{DataType, Field, Schema};
use criterion::async_executor::FuturesExecutor;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures_executor::block_on;
use futures_util::io::Cursor;
use futures_util::{pin_mut, TryStreamExt};
use itertools::Itertools;
use vortex::array::PrimitiveArray;
use vortex::compress::CompressionStrategy;
use vortex::compute::take;
use vortex::{Context, IntoArray};
use vortex_sampling_compressor::SamplingCompressor;
use vortex_serde::io::FuturesAdapter;
use vortex_serde::stream_reader::StreamArrayReader;
use vortex_serde::stream_writer::StreamArrayWriter;

fn ipc_take(c: &mut Criterion) {
    let mut group = c.benchmark_group("ipc_take");
    let indices = Int32Array::from(vec![10, 11, 12, 13, 100_000, 2_999_999]);
    group.bench_function("arrow", |b| {
        let mut buffer = vec![];
        {
            let field = Field::new("uid", DataType::Int32, true);
            let schema = Schema::new(vec![field]);
            let options = IpcWriteOptions::try_new(32, false, MetadataVersion::V5)
                .unwrap()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))
                .unwrap();
            let mut writer =
                ArrowStreamWriter::try_new_with_options(&mut buffer, &schema, options).unwrap();
            let array = Int32Array::from((0i32..3_000_000).rev().collect_vec());

            let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
            writer.write(&batch).unwrap();
        }

        b.iter(|| {
            let mut cursor = std::io::Cursor::new(&buffer);
            let mut reader = ArrowStreamReader::try_new(&mut cursor, None).unwrap();
            let batch = reader.next().unwrap().unwrap();
            let array_from_batch = batch.column(0);
            let array = array_from_batch
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            black_box(arrow_select::take::take(array, &indices, None).unwrap());
        });
    });

    group.bench_function("vortex", |b| {
        let indices = PrimitiveArray::from(vec![10, 11, 12, 13, 100_000, 2_999_999]).into_array();
        let uncompressed = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let ctx = Context::default();
        let compressor: &dyn CompressionStrategy = &SamplingCompressor::default();
        let compressed = compressor.compress(&uncompressed).unwrap();

        // Try running take over an ArrayView.
        let buffer =
            block_on(async { StreamArrayWriter::new(vec![]).write_array(compressed).await })
                .unwrap()
                .into_inner();

        let ctx_ref = &Arc::new(ctx);
        let ro_buffer = buffer.as_slice();
        let indices_ref = &indices;

        b.to_async(FuturesExecutor).iter(|| async move {
            let stream_reader =
                StreamArrayReader::try_new(FuturesAdapter(Cursor::new(ro_buffer)), ctx_ref.clone())
                    .await?
                    .load_dtype()
                    .await?;
            let reader = stream_reader.into_array_stream();
            pin_mut!(reader);
            let array_view = reader.try_next().await?.unwrap();
            black_box(take(&array_view, indices_ref))
        });
    });
}

criterion_group!(benches, ipc_take);
criterion_main!(benches);
