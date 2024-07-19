// fn bench_patched_take(c: &mut Criterion) {

use std::hint::black_box;

use arrow_buffer::BooleanBuffer;
use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rand::distributions::Open01;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use vortex_runend_bool::compress::runend_bool_encode_slice;

fn compress_compare(c: &mut Criterion) {
    compress_compare_param(c, 0.);
    compress_compare_param(c, 0.01);
    compress_compare_param(c, 0.40);
    compress_compare_param(c, 0.50);
    compress_compare_param(c, 0.60);
    compress_compare_param(c, 0.70);
    compress_compare_param(c, 0.80);
    compress_compare_param(c, 0.90);
    compress_compare_param(c, 0.95);
    compress_compare_param(c, 0.99);
    compress_compare_param(c, 1.);
}

fn compress_compare_param(c: &mut Criterion, sel_fac: f32) {
    let mut rng = StdRng::seed_from_u64(39451);
    let input = (0..1024 * 8 - 61)
        .map(|_x| rng.sample::<f32, _>(Open01) <= sel_fac)
        .collect_vec();
    let boolbuf = BooleanBuffer::from(input);

    let mut group = c.benchmark_group(format!("sel: {sel_fac}"));

    group.bench_function("ree bool", |b| {
        b.iter(|| black_box(runend_bool_encode_slice(&boolbuf)));
    });

    group.finish()
}

criterion_group!(benches, compress_compare);
criterion_main!(benches);
