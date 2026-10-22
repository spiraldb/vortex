// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Accumulating a nested field with the demand of its nullable parent, compared with accumulating the
//! logical field, whose validity already excludes the rows the parent nulls out.

#![allow(clippy::unwrap_used)]

use std::sync::LazyLock;

use divan::Bencher;
use mimalloc::MiMalloc;
use rand::prelude::*;
use vortex_array::ArrayRef;
use vortex_array::Columnar;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::AggregateDTypes;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::AggregateFnVTable;
use vortex_array::aggregate_fn::AggregateFnVTableExt;
use vortex_array::aggregate_fn::EmptyOptions;
use vortex_array::aggregate_fn::NumericalAggregateOpts;
use vortex_array::aggregate_fn::fns::count::Count;
use vortex_array::aggregate_fn::fns::is_constant::IsConstant;
use vortex_array::aggregate_fn::fns::max::Max;
use vortex_array::aggregate_fn::fns::min::Min;
use vortex_array::aggregate_fn::fns::null_count::NullCount;
use vortex_array::aggregate_fn::fns::sum::Sum;
use vortex_array::array_session;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::Nullability;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_mask::Mask;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    LazyLock::force(&SESSION);
    check_results();
    divan::main();
}

// Sized to keep the CodSpeed simulation under 1ms per benchmark.
const N: usize = 15_000;

static SESSION: LazyLock<VortexSession> = LazyLock::new(array_session);

const AGGREGATES: &[&str] = &["sum", "min", "max", "count", "null_count", "is_constant"];

fn aggregate(name: &str) -> AggregateFnRef {
    let opts = NumericalAggregateOpts::default();
    match name {
        "sum" => Sum.bind(opts),
        "min" => Min.bind(opts),
        "max" => Max.bind(opts),
        "count" => Count.bind(opts),
        "null_count" => NullCount.bind(EmptyOptions),
        "is_constant" => IsConstant.bind(EmptyOptions),
        _ => unreachable!("unknown aggregate {name}"),
    }
}

fn values() -> Buffer<i32> {
    let mut rng = StdRng::seed_from_u64(1);
    (0..N).map(|_| rng.random_range(-1000..1000)).collect()
}

/// The field's own validity: a tenth of its values are null.
fn field_validity() -> Mask {
    let mut rng = StdRng::seed_from_u64(5);
    Mask::from_iter((0..N).map(|_| rng.random_bool(0.9)))
}

/// A nullable struct field, holding values in every row including those the struct nulls out.
fn field() -> ArrayRef {
    PrimitiveArray::new(
        values(),
        Validity::from_mask(field_validity(), Nullability::Nullable),
    )
    .into_array()
}

/// Half the rows, chosen at random, as the struct validity of a nested field would be.
fn demand() -> Mask {
    let mut rng = StdRng::seed_from_u64(2);
    Mask::from_iter((0..N).map(|_| rng.random_bool(0.5)))
}

/// The logical field: null wherever the struct or the field is null.
fn masked_field() -> ArrayRef {
    PrimitiveArray::new(
        values(),
        Validity::from_mask(&field_validity() & &demand(), Nullability::Nullable),
    )
    .into_array()
}

fn run(aggregate_fn: &AggregateFnRef, batch: &ArrayRef, demand: Option<&Mask>) -> Scalar {
    let mut ctx = SESSION.create_execution_ctx();
    let mut acc = aggregate_fn.accumulator(batch.dtype()).unwrap();
    match demand {
        Some(demand) => acc.accumulate_demanded(batch, demand, &mut ctx).unwrap(),
        None => acc.accumulate(batch, &mut ctx).unwrap(),
    }
    acc.finish().unwrap()
}

/// Check that every benchmark computes the right result before timing it.
///
/// Undemanded rows are null, so the demanded field gives the same result as the logical field for
/// every aggregate. The null count counts the field's own nulls and the rows the struct nulls out.
fn check_results() {
    let (field, demand) = (field(), demand());
    for name in AGGREGATES {
        let aggregate_fn = aggregate(name);
        assert_eq!(
            run(&aggregate_fn, &field, Some(&demand)),
            run(&aggregate_fn, &masked_field(), None),
            "{name}"
        );
    }

    let expected_nulls = (!&(&field_validity() & &demand)).true_count() as u64;
    let null_count = run(&aggregate("null_count"), &field, Some(&demand));
    assert_eq!(
        null_count.as_primitive().typed_value::<u64>(),
        Some(expected_nulls)
    );
}

/// Accumulate the field with the demand of its parent.
#[divan::bench(args = AGGREGATES)]
fn demanded(bencher: Bencher, name: &str) {
    let aggregate_fn = aggregate(name);
    let batch = field();
    let demand = demand();
    bencher
        .with_inputs(|| {
            (
                aggregate_fn.accumulator(batch.dtype()).unwrap(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_values(|(mut acc, mut ctx)| {
            acc.accumulate_demanded(&batch, &demand, &mut ctx).unwrap();
            acc.flush().unwrap()
        });
}

/// Accumulate the logical field, whose validity already excludes the undemanded rows.
#[divan::bench(args = AGGREGATES)]
fn nullable(bencher: Bencher, name: &str) {
    let aggregate_fn = aggregate(name);
    bencher
        .with_inputs(|| {
            // A fresh batch per iteration, so no statistic cached by a previous one is reused.
            let batch = masked_field();
            (
                aggregate_fn.accumulator(batch.dtype()).unwrap(),
                batch,
                SESSION.create_execution_ctx(),
            )
        })
        .bench_values(|(mut acc, batch, mut ctx)| {
            acc.accumulate(&batch, &mut ctx).unwrap();
            acc.flush().unwrap()
        });
}

const DENSITIES: &[f64] = &[0.01, 0.5, 0.99];

fn demand_with_density(density: f64) -> Mask {
    let mut rng = StdRng::seed_from_u64(3);
    Mask::from_iter((0..N).map(|_| rng.random_bool(density)))
}

/// The minimum of the demanded rows, by demand density, for a canonical and a dictionary batch.
#[divan::bench(args = DENSITIES, consts = [false, true])]
fn min_demanded_by_density<const DICT: bool>(bencher: Bencher, density: f64) {
    let batch = density_batch(DICT);
    let demand = demand_with_density(density);
    let aggregate_fn = aggregate("min");
    bencher
        .with_inputs(|| {
            (
                aggregate_fn.accumulator(batch.dtype()).unwrap(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_values(|(mut acc, mut ctx)| {
            acc.accumulate_demanded(&batch, &demand, &mut ctx).unwrap();
            acc.flush().unwrap()
        });
}

fn density_batch(dict: bool) -> ArrayRef {
    if dict {
        let mut rng = StdRng::seed_from_u64(4);
        let codes: Buffer<u8> = (0..N).map(|_| rng.random_range(0..64)).collect();
        let values: Buffer<i32> = (0..64).map(|v| v * 7 - 100).collect();
        vortex_array::arrays::DictArray::new(codes.into_array(), values.into_array()).into_array()
    } else {
        PrimitiveArray::new(values(), Validity::NonNullable).into_array()
    }
}

const VALID_DENSITIES: &[f64] = &[0.5, 0.9, 0.95, 0.97, 0.99];

/// A nullable batch whose given fraction of rows, chosen at random, is valid.
fn batch_with_validity(density: f64) -> (ArrayRef, Mask) {
    let mut rng = StdRng::seed_from_u64(6);
    let valid = Mask::from_iter((0..N).map(|_| rng.random_bool(density)));
    let batch = PrimitiveArray::new(
        values(),
        Validity::from_mask(valid.clone(), Nullability::Nullable),
    )
    .into_array();
    (batch, valid)
}

/// Run the vtable's kernel over `batch` directly, bypassing the accumulator's valid-only filter.
fn run_kernel<V: AggregateFnVTable>(
    vtable: &V,
    options: &V::Options,
    batch: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> Scalar {
    let dtypes = AggregateDTypes::try_new(vtable, options, batch.dtype().clone()).unwrap();
    let mut partial = vtable.empty_partial(dtypes.args(options)).unwrap();
    let columnar = batch.clone().execute::<Columnar>(ctx).unwrap();
    vtable
        .accumulate(dtypes.args(options), &mut partial, &columnar, ctx)
        .unwrap();
    vtable.to_scalar(dtypes.args(options), &partial).unwrap()
}

/// Compare the nullable kernel with filtering to the valid rows first, checking that both agree.
fn bench_valid_only<V: AggregateFnVTable>(
    bencher: Bencher,
    vtable: V,
    options: V::Options,
    density: f64,
    filter_first: bool,
) {
    let (batch, valid) = batch_with_validity(density);
    let mut ctx = SESSION.create_execution_ctx();
    assert_eq!(
        run_kernel(&vtable, &options, &batch, &mut ctx),
        run_kernel(
            &vtable,
            &options,
            &batch.filter(valid.clone()).unwrap(),
            &mut ctx
        ),
    );
    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_values(|mut ctx| {
            if filter_first {
                let filtered = batch.filter(valid.clone()).unwrap();
                run_kernel(&vtable, &options, &filtered, &mut ctx)
            } else {
                run_kernel(&vtable, &options, &batch, &mut ctx)
            }
        });
}

/// The nullable kernel over every row, null rows included.
#[divan::bench(args = VALID_DENSITIES)]
fn sum_nullable_kernel(bencher: Bencher, density: f64) {
    bench_valid_only(
        bencher,
        Sum,
        NumericalAggregateOpts::default(),
        density,
        false,
    );
}

/// Filter to the valid rows first, then the dense kernel.
#[divan::bench(args = VALID_DENSITIES)]
fn sum_filter_first(bencher: Bencher, density: f64) {
    bench_valid_only(
        bencher,
        Sum,
        NumericalAggregateOpts::default(),
        density,
        true,
    );
}

#[divan::bench(args = VALID_DENSITIES)]
fn min_nullable_kernel(bencher: Bencher, density: f64) {
    bench_valid_only(
        bencher,
        Min,
        NumericalAggregateOpts::default(),
        density,
        false,
    );
}

#[divan::bench(args = VALID_DENSITIES)]
fn min_filter_first(bencher: Bencher, density: f64) {
    bench_valid_only(
        bencher,
        Min,
        NumericalAggregateOpts::default(),
        density,
        true,
    );
}

#[divan::bench(args = VALID_DENSITIES)]
fn max_nullable_kernel(bencher: Bencher, density: f64) {
    bench_valid_only(
        bencher,
        Max,
        NumericalAggregateOpts::default(),
        density,
        false,
    );
}

#[divan::bench(args = VALID_DENSITIES)]
fn max_filter_first(bencher: Bencher, density: f64) {
    bench_valid_only(
        bencher,
        Max,
        NumericalAggregateOpts::default(),
        density,
        true,
    );
}
