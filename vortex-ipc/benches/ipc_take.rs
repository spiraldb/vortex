use std::io::Cursor;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use vortex::array::primitive::PrimitiveArray;
use vortex::array::Array;
use vortex::serde::{ReadCtx, WriteCtx};
use vortex_array2::array::primitive::PrimitiveData;
use vortex_array2::compute::take::take;
use vortex_array2::{IntoArray, SerdeContext, WithArray};
use vortex_ipc::iter::FallibleLendingIterator;
use vortex_ipc::reader::StreamReader;
use vortex_ipc::writer::StreamWriter;

fn ipc_take(c: &mut Criterion) {
    let indices = PrimitiveData::from(vec![10, 11, 12, 13, 100_000, 2_999_999]).into_array();
    let data = PrimitiveData::from(vec![5; 3_000_000]).into_array();

    c.bench_function("take_data", |b| {
        b.iter(|| black_box(take(&data, &indices).unwrap()));
    });

    // Try running take over an ArrayView.
    let mut buffer = vec![];
    {
        let mut cursor = Cursor::new(&mut buffer);
        let mut writer = StreamWriter::try_new(&mut cursor, SerdeContext::default()).unwrap();
        data.with_array(|a| writer.write(a)).unwrap();
    }

    c.bench_function("take_view", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(&buffer);
            let mut reader = StreamReader::try_new(&mut cursor).unwrap();
            let mut array_reader = reader.next().unwrap().unwrap();
            let array_view = array_reader.next().unwrap().unwrap().into_array();
            black_box(take(&array_view, &indices).unwrap())
        });
    });

    // Try the old way of taking data.
    let arr = PrimitiveArray::from(vec![5; 3_000_000]);
    let indices = PrimitiveArray::from(vec![10, 11, 12, 13, 100_000, 2_999_999]);

    let mut buffer = vec![];
    {
        let mut cursor = Cursor::new(&mut buffer);
        let mut ctx = WriteCtx::new(&mut cursor);
        arr.serde().unwrap().write(&mut ctx).unwrap();
    }

    c.bench_function("take_old", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(&buffer);
            let mut ctx = ReadCtx::new(arr.dtype(), &mut cursor);
            let arr = ctx.read().unwrap();
            black_box(vortex::compute::take::take(arr.as_ref(), &indices).unwrap())
        });
    });
}

criterion_group!(benches, ipc_take);
criterion_main!(benches);
