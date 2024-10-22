#![allow(clippy::unwrap_used)]

use criterion::{criterion_group, criterion_main, Criterion};
use vortex::array::{PrimitiveArray, VarBinArray};
use vortex::compute::take;
use vortex::validity::Validity;
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_dtype::{DType, Nullability};

// Try take with different array frequency.
fn fixture(len: usize) -> VarBinArray {
    let values: [Option<&'static str>; 3] =
        [Some("inlined"), None, Some("verylongstring--notinlined")];

    VarBinArray::from_iter(
        values.into_iter().cycle().take(len),
        DType::Utf8(Nullability::Nullable),
    )
}

// What fraction of the indices to take.
fn indices(len: usize) -> Array {
    PrimitiveArray::from_vec(
        (0..len)
            .filter_map(|x| (x % 2 == 0).then_some(x as u64))
            .collect(),
        Validity::NonNullable,
    )
    .into_array()
}

fn bench_varbin(c: &mut Criterion) {
    let array = fixture(65_535);
    let indices = indices(1024);

    c.bench_function("varbin", |b| b.iter(|| take(&array, &indices).unwrap()));
}

fn bench_varbinview(c: &mut Criterion) {
    let array = fixture(65_535).into_varbinview().unwrap();
    let indices = indices(1024);

    c.bench_function("varbinview", |b| {
        b.iter(|| take(array.as_ref(), &indices).unwrap())
    });
}

criterion_group!(bench_take, bench_varbin, bench_varbinview);
criterion_main!(bench_take);
