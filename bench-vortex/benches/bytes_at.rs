//! Benchmark for the `bytes_at` operation on a VarBinView.
//! This measures the performance of accessing an individual byte-slice in a VarBinViewArray.

use std::io::Cursor;
use std::sync::Arc;

use codspeed_criterion_compat::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::executor::block_on;
use futures::StreamExt;
use vortex::array::{PrimitiveArray, VarBinArray};
use vortex::validity::Validity;
use vortex::{Context, IntoArray, IntoCanonical};
use vortex_dtype::{DType, Nullability};
use vortex_serde::stream_reader::StreamArrayReader;
use vortex_serde::stream_writer::StreamArrayWriter;

fn array_data_fixture() -> VarBinArray {
    VarBinArray::try_new(
        PrimitiveArray::from(vec![0i32, 5i32, 10i32, 15i32, 20i32]).into_array(),
        PrimitiveArray::from(b"helloworldhelloworld".to_vec()).into_array(),
        DType::Utf8(Nullability::NonNullable),
        Validity::NonNullable,
    )
    .unwrap()
}

fn array_view_fixture() -> VarBinArray {
    let array_data = array_data_fixture();
    let mut buffer = Vec::new();

    let writer = StreamArrayWriter::new(&mut buffer);
    block_on(writer.write_array(array_data.into_array())).unwrap();

    let ctx = Arc::new(Context::default());
    let reader = block_on(StreamArrayReader::try_new(Cursor::new(buffer), ctx.clone())).unwrap();
    let reader = block_on(reader.load_dtype()).unwrap();

    let mut stream = Box::pin(reader.into_array_stream());

    block_on(stream.next())
        .unwrap()
        .unwrap()
        .into_canonical()
        .unwrap()
        .into_varbin()
        .unwrap()
}

fn benchmark(c: &mut Criterion) {
    c.bench_with_input(
        BenchmarkId::new("bytes_at", "array_data"),
        &array_data_fixture(),
        |b, array| {
            b.iter(|| array.bytes_at(3));
        },
    );

    c.bench_with_input(
        BenchmarkId::new("bytes_at", "array_data"),
        &array_view_fixture(),
        |b, array| {
            b.iter(|| array.bytes_at(3));
        },
    );
}

criterion_group!(bench_bytes_at, benchmark);
criterion_main!(bench_bytes_at);
