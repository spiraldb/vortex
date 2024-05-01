use std::cell::RefCell;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;

use arrow::ipc::reader::StreamReader as ArrowStreamReader;
use arrow_array::{ Int32Array, RecordBatch};
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter as ArrowStreamWriter};
use arrow_ipc::{CompressionType, MetadataVersion};
use arrow_schema::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use criterion::async_executor::{AsyncExecutor};
use itertools::Itertools;
use monoio::{Driver, FusionDriver, FusionRuntime, RuntimeBuilder};
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::Compressor;
use vortex::compute::take::take;
use vortex::{Array, Context, IntoArray, OwnedArray};
use vortex_error::VortexResult;
use vortex_ipc::iter::FallibleLendingIterator;
use vortex_ipc::reader::StreamReader;
use vortex_ipc::writer::StreamWriter;

pub struct MonoioExecutor<D: Driver>(pub RefCell<FusionRuntime<D>>);

impl<D: Driver> AsyncExecutor for MonoioExecutor<D> {
    fn block_on<T>(&self, future: impl Future<Output=T>) -> T {
        self.0.borrow_mut().block_on(future)
    }
}

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
            let mut cursor = Cursor::new(&buffer);
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
        let compressed = Compressor::new(&ctx).compress(&uncompressed, None).unwrap();

        // Try running take over an ArrayView.
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            let mut writer = StreamWriter::try_new(&mut cursor, &ctx).unwrap();
            writer.write_array(&compressed).unwrap();
        }
        let rt = RuntimeBuilder::<FusionDriver>::new().build()
            .expect("Unable to build runtime");
        let executor = MonoioExecutor(RefCell::new(rt));

        b.to_async(executor).iter(|| bench_vortex_read(&buffer, &indices, &ctx));
    });
}

async fn bench_vortex_read(buf: &Vec<u8>, indices: &Array<'_>, ctx: &Context) -> VortexResult<OwnedArray> {
    let mut reader = StreamReader::try_new(buf.as_slice(), ctx).await.unwrap();
    let mut array_reader = reader.next().await.unwrap().unwrap();
    let array_view = array_reader.next().await.unwrap().unwrap();
    black_box(take(&array_view, indices))
}

criterion_group!(benches, ipc_take);
criterion_main!(benches);
