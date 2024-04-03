use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::distributions::{Alphanumeric, Uniform};
use rand::{thread_rng, Rng};
use vortex_fastlanes::{bitpack_primitive, unpack_primitive};

fn values(len: usize, bits: usize) -> Vec<u32> {
    let mut rng = thread_rng();
    let range = Uniform::new(0_u32, 2_u32.pow(bits as u32));
    rng.sample_iter(range).take(len).collect()
}

fn dict_encode(c: &mut Criterion) {
    let bits: usize = 8;
    let values = values(1_000_000, bits);

    c.bench_function("bitpack_primitive", |b| {
        b.iter(|| black_box(bitpack_primitive(&values, bits)));
    });
    
    let packed = bitpack_primitive(&values, bits);
    c.bench_function("unpack_primitive", |b| {
        b.iter(|| black_box(unpack_primitive(&packed, bits)));
    });
}

criterion_group!(benches, dict_encode);
criterion_main!(benches);
