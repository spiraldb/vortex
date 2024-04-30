use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use vortex::array::chunked::ChunkedArray;
use vortex::{IntoArray, ToArray, ToStatic};
use vortex_dtype::{DType, Nullability, Signedness};

fn scalar_subtract(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalar_subtract");

    let mut rng = thread_rng();
    let range = Uniform::new(0, 100_000_000);
    let data1: Vec<i64> = (0..10_000_000).map(|_| rng.sample(range)).collect();
    let data2: Vec<i64> = (0..10_000_000).map(|_| rng.sample(range)).collect();


    let to_subtract = -1i32;

    let chunked = ChunkedArray::try_new(
        vec![data1.into_array(), data2.into_array()],
        DType::Int(64.into(), Signedness::Signed, Nullability::NonNullable),
    )
        .unwrap()
        .to_array()
        .to_static();


    group.bench_function("vortex", |b| {
        b.iter(|| {
            let array = vortex::compute::scalar_subtract::scalar_subtract(&chunked, to_subtract).unwrap();

            let chunked = ChunkedArray::try_from(array).unwrap();
            let mut chunks_out = chunked.chunks();
            let results = chunks_out.next().unwrap()
                .flatten_primitive()
                .unwrap()
                .typed_data::<i64>()
                .to_vec();
            black_box(results)
        });
    });
}

criterion_group!(benches, scalar_subtract);
criterion_main!(benches);
