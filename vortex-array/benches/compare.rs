use criterion::{black_box, criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use vortex::array::BoolArray;
use vortex::IntoArray;
use vortex_error::VortexError;
use vortex_expr::Operator;

fn filter_bool_indices(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare");

    let mut rng = thread_rng();
    let range = Uniform::new(0u8, 1);
    let arr = BoolArray::from(
        (0..10_000_000)
            .map(|_| rng.sample(range) == 0)
            .collect_vec(),
    )
    .into_array();
    let arr2 = BoolArray::from(
        (0..10_000_000)
            .map(|_| rng.sample(range) == 0)
            .collect_vec(),
    )
    .into_array();

    group.bench_function("compare_bool", |b| {
        b.iter(|| {
            let indices = vortex::compute::compare(&arr, &arr2, Operator::Gte).unwrap();
            black_box(indices);
            Ok::<(), VortexError>(())
        });
    });
}

fn filter_indices(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare");

    let mut rng = thread_rng();
    let range = Uniform::new(0i64, 100_000_000);
    let arr = (0..10_000_000)
        .map(|_| rng.sample(range))
        .collect_vec()
        .into_array();

    let arr2 = (0..10_000_000)
        .map(|_| rng.sample(range))
        .collect_vec()
        .into_array();

    group.bench_function("compare_int", |b| {
        b.iter(|| {
            let indices = vortex::compute::compare(&arr, &arr2, Operator::Gte).unwrap();
            black_box(indices);
            Ok::<(), VortexError>(())
        });
    });
}

criterion_group!(benches, filter_indices, filter_bool_indices);
criterion_main!(benches);
