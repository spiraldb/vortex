// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{
    distributions::{Distribution, Uniform},
    SeedableRng,
};
use rand_chacha::ChaCha8Rng;

use codecz::encodings::ree;

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

    let encoded = codecz::alp::encode_with(&data, exp).unwrap();
    println!(
        "num_exceptions: {}, exponents: {:?}",
        encoded.num_exceptions, exp
    );

    c.bench_function("alp decode", |b| {
        b.iter(|| {
            let decoded =
                codecz::alp::decode::<f64>(black_box(&encoded.values), black_box(exp)).unwrap();
            black_box(decoded);
        })
    });
}

criterion_group!(benches, ree_benchmark, alp_benchmark);
criterion_main!(benches);
