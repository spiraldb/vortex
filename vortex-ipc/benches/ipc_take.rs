use std::io::Cursor;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::take::take;
use vortex::{IntoArray, SerdeContext};
use vortex_ipc::iter::FallibleLendingIterator;
use vortex_ipc::reader::StreamReader;
use vortex_ipc::writer::StreamWriter;

fn ipc_take(c: &mut Criterion) {
    let indices = PrimitiveArray::from(vec![10, 11, 12, 13, 100_000, 2_999_999]).into_array();
    let data = PrimitiveArray::from(vec![5; 3_000_000]).into_array();
    //
    // c.bench_function("take_data", |b| {
    //     b.iter(|| black_box(take(&data, &indices).unwrap()));
    // });

    // Try running take over an ArrayView.
    let mut buffer = vec![];
    {
        let mut cursor = Cursor::new(&mut buffer);
        let mut writer = StreamWriter::try_new(&mut cursor, SerdeContext::default()).unwrap();
        writer.write_array(&data).unwrap();
        writer.write_array(&data).unwrap();
    }

    c.bench_function("take_view", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(&buffer);
            let mut reader = StreamReader::try_new(&mut cursor).unwrap();
            let mut array_reader = reader.next().unwrap().unwrap();
            while let Some(array_chunk) = array_reader.next().unwrap() {
                black_box(take(&array_chunk, &indices).unwrap());
            }
        });
    });
}

criterion_group!(benches, ipc_take);
criterion_main!(benches);
