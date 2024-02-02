use criterion::{black_box, criterion_group, criterion_main, Criterion};

use enc::array::primitive::PrimitiveArray;
use enc::compute;

fn zigzag_benchmark(c: &mut Criterion) {
    let mut data = vec![0, 1, -1, i32::MAX, i32::MIN];
    data.extend(0..100_000);
    let data = data; // discard mut

    c.bench_function("zigzag encode", |b| {
        b.iter(|| {
            let encoded =
                PrimitiveArray::from_vec_in(codecz::zigzag::encode(black_box(&data)).unwrap());
            black_box(encoded);
        })
    });

    let enc_data = PrimitiveArray::from_vec(data.clone());
    c.bench_function("enc zigzag encode", |b| {
        b.iter(|| {
            let encoded = compute::compress::zigzag::zigzag_encode(black_box(&enc_data));
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

    let enc_data_encoded = compute::compress::zigzag::zigzag_encode(&enc_data);
    c.bench_function("enc zigzag decode", |b| {
        b.iter(|| {
            let decoded = compute::compress::zigzag::zigzag_decode(black_box(&enc_data_encoded));
            black_box(decoded);
        })
    });
}

criterion_group!(benches, zigzag_benchmark);
criterion_main!(benches);
