#![allow(clippy::unwrap_used)]

use criterion::{criterion_group, criterion_main, Criterion};
use vortex::array::VarBinArray;
use vortex::{IntoArray, IntoCanonical};
use vortex_dict::{dict_encode_varbin, DictArray};
use vortex_dtype::{DType, Nullability};

fn fixture(len: usize) -> DictArray {
    let values = [
        Some("inlined"),
        None,
        Some("not inlined but repeated often"),
    ];

    let strings = VarBinArray::from_iter(
        values.into_iter().cycle().take(len),
        DType::Utf8(Nullability::Nullable),
    );

    let (codes, values) = dict_encode_varbin(&strings);
    DictArray::try_new(codes.into_array(), values.into_array()).unwrap()
}

fn bench_canonical(c: &mut Criterion) {
    let dict_array = fixture(1024 * 1024).into_array();

    c.bench_function("canonical", |b| {
        b.iter(|| dict_array.clone().into_canonical())
    });
}

criterion_group!(bench_dict_canonical, bench_canonical);
criterion_main!(bench_dict_canonical);
