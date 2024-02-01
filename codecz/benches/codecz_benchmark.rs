use codecz::encodings::ree;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{
    distributions::{Distribution, Uniform},
    SeedableRng,
};
use rand_chacha::ChaCha8Rng;

fn ree_benchmark(c: &mut Criterion) {
    let mut rng = ChaCha8Rng::seed_from_u64(0);
    let range = Uniform::new(0, 10);
    let data: Vec<u8> = (0..10000)
        .flat_map(|_| {
            let repeat = range.sample(&mut rng) as u8;
            let vec = vec![repeat; 10];
            vec
        })
        .collect();

    c.bench_function("ree encode", |b| {
        b.iter(|| {
            let (values, run_ends) = ree::encode(black_box(&data)).unwrap();
            black_box(values);
            black_box(run_ends);
        })
    });

    let (values, run_ends) = ree::encode(&data).unwrap();
    c.bench_function("ree decode", |b| {
        b.iter(|| {
            let decoded = ree::decode(black_box(&values), black_box(&run_ends)).unwrap();
            black_box(decoded);
        })
    });
}

fn zigzag_benchmark(c: &mut Criterion) {
    let mut rng = ChaCha8Rng::seed_from_u64(0);
    let range = Uniform::new(i32::MIN, i32::MAX);
    let mut data: Vec<i32> = (0..100000).map(|_| range.sample(&mut rng)).collect();
    data.append(&mut vec![0, 1, -1, i32::MAX, i32::MIN]);
    let data = data; // discard mut

    c.bench_function("zigzag encode", |b| {
        b.iter(|| {
            let encoded = codecz::zigzag::encode(black_box(&data)).unwrap();
            black_box(encoded);
        })
    });

    let encoded = codecz::zigzag::encode(&data).unwrap();
    c.bench_function("zigzag decode", |b| {
        b.iter(|| {
            let decoded = codecz::zigzag::decode::<i32>(black_box(&encoded)).unwrap();
            black_box(decoded);
        })
    });
}

fn alp_benchmark(c: &mut Criterion) {
    let mut rng = ChaCha8Rng::seed_from_u64(0);
    let range = Uniform::new(i32::MIN, i32::MAX);
    let exp_range = Uniform::new(-4, 0);
    let data = (0..100000)
        .map(|_| {
            let mantissa = range.sample(&mut rng) as f64;
            let exponent = exp_range.sample(&mut rng);
            mantissa * 10_f64.powi(exponent)
        })
        .collect::<Vec<f64>>();

    c.bench_function("alp prelude", |b| {
        b.iter(|| {
            let exponents = codecz::alp::find_exponents(black_box(&data)).unwrap();
            black_box(exponents);
        })
    });

    let exp = codecz::alp::find_exponents(&data).unwrap();
    c.bench_function("alp encode", |b| {
        b.iter(|| {
            let encoded = codecz::alp::encode_with(black_box(&data), exp).unwrap();
            black_box(encoded);
        })
    });

    let (values, exceptions_idx) = codecz::alp::encode_with(&data, exp).unwrap();
    println!(
        "num_exceptions: {}, exponents: {:?}",
        exceptions_idx
            .iter()
            .map(|b| b.count_ones() as u64)
            .sum::<u64>(),
        exp
    );

    c.bench_function("alp decode", |b| {
        b.iter(|| {
            let decoded = codecz::alp::decode::<f64>(black_box(&values), black_box(exp)).unwrap();
            black_box(decoded);
        })
    });
}

criterion_group!(benches, ree_benchmark, zigzag_benchmark, alp_benchmark);
criterion_main!(benches);
