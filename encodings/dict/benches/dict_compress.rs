#![allow(clippy::unwrap_used)]

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rand::distributions::{Alphanumeric, Uniform};
use rand::prelude::SliceRandom;
use rand::{thread_rng, Rng};
use vortex::array::{PrimitiveArray, VarBinArray};
use vortex::compute::take;
use vortex::{ArrayTrait, IntoArray as _, IntoCanonical as _};
use vortex_dict::{dict_encode_primitive, dict_encode_typed_primitive, dict_encode_varbin, DictArray};
use vortex_dtype::match_each_native_ptype;

fn gen_primitive_dict(len: usize, uniqueness: f64) -> PrimitiveArray {
    let mut rng = thread_rng();
    let value_range = len as f64 * uniqueness;
    let range = Uniform::new(-(value_range / 2.0) as i32, (value_range / 2.0) as i32);
    let data: Vec<i32> = (0..len).map(|_| rng.sample(range)).collect();

    PrimitiveArray::from(data)
}

fn gen_varbin_dict(len: usize, uniqueness: f64) -> VarBinArray {
    let mut rng = thread_rng();
    let uniq_cnt = (len as f64 * uniqueness) as usize;
    let dict: Vec<String> = (0..uniq_cnt)
        .map(|_| {
            (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(16)
                .map(char::from)
                .collect()
        })
        .collect();
    let words: Vec<&str> = (0..len)
        .map(|_| dict.choose(&mut rng).unwrap().as_str())
        .collect();
    VarBinArray::from(words)
}

fn dict_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("dict_encode");

    let primitive_arr = gen_primitive_dict(1_000_000, 0.00005);
    group.throughput(Throughput::Bytes(primitive_arr.nbytes() as u64));
    group.bench_function("dict_encode_primitives", |b| {
        b.iter(|| black_box(dict_encode_primitive(&primitive_arr)));
    });

    let varbin_arr = gen_varbin_dict(1_000_000, 0.00005);
    group.throughput(Throughput::Bytes(varbin_arr.nbytes() as u64));
    group.bench_function("dict_encode_varbin", |b| {
        b.iter(|| black_box(dict_encode_varbin(&varbin_arr)));
    });
}

fn dict_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("dict_decode");

    let primitive_arr = gen_primitive_dict(1_000_000, 0.00005);
    let (codes, values) = dict_encode_primitive(&primitive_arr);
    group.throughput(Throughput::Bytes(primitive_arr.nbytes() as u64));
    group.bench_function("dict_decode_primitives", |b| {
        b.iter_batched(
            || DictArray::try_new(codes.clone().into_array(), values.clone().into_array()).unwrap(),
            |dict_arr| {
                black_box(dict_arr.into_canonical().unwrap())
            },
            BatchSize::SmallInput,
        );
    });

    let varbin_arr = gen_varbin_dict(1_000_000, 0.00005);
    let (codes, values) = vortex_dict::dict_encode_varbin(&varbin_arr);
    group.throughput(Throughput::Bytes(varbin_arr.nbytes() as u64));
    group.bench_function("dict_decode_varbin", |b| {
        b.iter_batched(
            || DictArray::try_new(codes.clone().into_array(), values.clone().into_array()).unwrap(),
            |dict_arr| {
                black_box(dict_arr.into_canonical().unwrap())
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, dict_encode, dict_decode);
criterion_main!(benches);
