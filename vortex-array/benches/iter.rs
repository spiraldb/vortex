use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use itertools::Itertools;
use vortex::array::PrimitiveArray;
use vortex::validity::Validity;
use vortex::variants::ArrayVariants;

fn std_iter(c: &mut Criterion) {
    let data = (0_u32..1_000_000).map(Some).collect_vec();
    c.bench_function("std_iter", |b| {
        b.iter_batched(|| data.clone().into_iter(), do_work, BatchSize::SmallInput)
    });
}

fn vortex_iter(c: &mut Criterion) {
    let data = PrimitiveArray::from_vec((0_u32..1_000_000).collect_vec(), Validity::AllValid);
    c.bench_function("vortex_iter", |b| {
        b.iter_batched(
            || {
                data.clone()
                    .as_primitive_array()
                    .unwrap()
                    .unsigned32_iter()
                    .unwrap()
            },
            do_work,
            BatchSize::SmallInput,
        )
    });
}

fn arrow_iter(c: &mut Criterion) {
    let data = arrow_array::UInt32Array::from_iter(0_u32..1_000_000);
    c.bench_function("arrow_iter", |b| {
        b.iter_batched(|| data.iter(), do_work, BatchSize::SmallInput)
    });
}

fn do_work(iter: impl Iterator<Item = Option<u32>>) {
    for _i in iter {
        criterion::black_box(())
    }
}

criterion_group!(benches, std_iter, vortex_iter, arrow_iter);
criterion_main!(benches);
