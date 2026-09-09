// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::LazyLock;

use divan::Bencher;
use rand::prelude::*;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::fns::sum_v2::sum_v2;
use vortex_array::array_session;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::DecimalType;
use vortex_array::expr::stats::Stat;
use vortex_array::match_each_decimal_value_type;
use vortex_array::scalar::DecimalValue;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_error::VortexExpect;
use vortex_session::VortexSession;

fn main() {
    LazyLock::force(&SESSION);
    divan::main();
}

// Sized to keep the CodSpeed simulation under 1ms per benchmark.
const N: usize = 15_000;

static SESSION: LazyLock<VortexSession> = LazyLock::new(array_session);

#[divan::bench(args = [8, 28, 66])]
fn sum_v2_decimal(bencher: Bencher, precision: u8) {
    bench_decimal_sum(bencher, precision, false);
}

#[divan::bench(args = [8, 28, 66])]
fn sum_v2_decimal_nulls(bencher: Bencher, precision: u8) {
    bench_decimal_sum(bencher, precision, true);
}

fn bench_decimal_sum(bencher: Bencher, precision: u8, nullable: bool) {
    let dtype = DecimalDType::new(precision, 2);
    let values_type = DecimalType::smallest_decimal_value_type(&dtype);
    let array = match_each_decimal_value_type!(values_type, |I| {
        let values = (0..N)
            .map(|i| {
                DecimalValue::I64(i as i64 % 1000 - 500)
                    .cast::<I>()
                    .vortex_expect("benchmark value fits decimal storage")
            })
            .collect::<Buffer<_>>();
        let validity = if nullable {
            Validity::from_iter((0..N).map(|i| i % 5 != 0))
        } else {
            Validity::NonNullable
        };
        DecimalArray::new(values, dtype, validity).into_array()
    });
    bencher
        .with_inputs(|| (array.clone(), SESSION.create_execution_ctx()))
        .bench_refs(|(array, ctx)| sum_v2(array, ctx));
}

#[divan::bench]
fn sum_i32(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(1);
    let data: Vec<i32> = (0..N).map(|_| rng.random_range(-1000..1000)).collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| a.statistics().compute_as::<i64>(Stat::Sum, ctx));
}

#[divan::bench]
fn sum_v2_i32(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(1);
    let data: Vec<i32> = (0..N).map(|_| rng.random_range(-1000..1000)).collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| sum_v2(a, ctx));
}

#[divan::bench]
fn sum_u32(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(2);
    let data: Vec<u32> = (0..N).map(|_| rng.random_range(0..2000)).collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| a.statistics().compute_as::<u64>(Stat::Sum, ctx));
}

#[divan::bench]
fn sum_v2_u32(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(2);
    let data: Vec<u32> = (0..N).map(|_| rng.random_range(0..2000)).collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| sum_v2(a, ctx));
}

#[divan::bench]
fn sum_i64(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(3);
    let data: Vec<i64> = (0..N).map(|_| rng.random_range(-1000..1000)).collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| a.statistics().compute_as::<i64>(Stat::Sum, ctx));
}

#[divan::bench]
fn sum_v2_i64(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(3);
    let data: Vec<i64> = (0..N).map(|_| rng.random_range(-1000..1000)).collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| sum_v2(a, ctx));
}

#[divan::bench]
fn sum_f64(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(6);
    let data: Vec<f64> = (0..N).map(|_| rng.random_range(-1000.0..1000.0)).collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| a.statistics().compute_as::<f64>(Stat::Sum, ctx));
}

#[divan::bench]
fn sum_v2_f64(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(6);
    let data: Vec<f64> = (0..N).map(|_| rng.random_range(-1000.0..1000.0)).collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| sum_v2(a, ctx));
}

#[divan::bench]
fn sum_f64_nulls_clustered(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(7);
    let data: Vec<Option<f64>> = (0..N)
        .map(|i| {
            if (i / 64) % 10 == 0 {
                None
            } else {
                Some(rng.random_range(-1000.0..1000.0))
            }
        })
        .collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_option_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| a.statistics().compute_as::<f64>(Stat::Sum, ctx));
}

#[divan::bench]
fn sum_v2_f64_nulls_clustered(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(7);
    let data: Vec<Option<f64>> = (0..N)
        .map(|i| {
            if (i / 64) % 10 == 0 {
                None
            } else {
                Some(rng.random_range(-1000.0..1000.0))
            }
        })
        .collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_option_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| sum_v2(a, ctx));
}

// Clustered nulls: long runs of valid values broken up by occasional null blocks. This is the
// case the run-based valid path is expected to accelerate.
#[divan::bench]
fn sum_i32_nulls_clustered(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(4);
    let data: Vec<Option<i32>> = (0..N)
        .map(|i| {
            if (i / 64) % 10 == 0 {
                None
            } else {
                Some(rng.random_range(-1000..1000))
            }
        })
        .collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_option_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| a.statistics().compute_as::<i64>(Stat::Sum, ctx));
}

#[divan::bench]
fn sum_v2_i32_nulls_clustered(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(4);
    let data: Vec<Option<i32>> = (0..N)
        .map(|i| {
            if (i / 64) % 10 == 0 {
                None
            } else {
                Some(rng.random_range(-1000..1000))
            }
        })
        .collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_option_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| sum_v2(a, ctx));
}

// Scattered nulls: ~50% nulls placed at random, producing many short runs. This is the worst case
// for a run-based valid path, used to guard against regressions versus a per-element loop.
#[divan::bench]
fn sum_i32_nulls_scattered(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(5);
    let data: Vec<Option<i32>> = (0..N)
        .map(|_| rng.random_bool(0.5).then(|| rng.random_range(-1000..1000)))
        .collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_option_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| a.statistics().compute_as::<i64>(Stat::Sum, ctx));
}

#[divan::bench]
fn sum_v2_i32_nulls_scattered(bencher: Bencher) {
    let mut rng = StdRng::seed_from_u64(5);
    let data: Vec<Option<i32>> = (0..N)
        .map(|_| rng.random_bool(0.5).then(|| rng.random_range(-1000..1000)))
        .collect();
    bencher
        .with_inputs(|| {
            (
                PrimitiveArray::from_option_iter(data.iter().copied()).into_array(),
                SESSION.create_execution_ctx(),
            )
        })
        .bench_refs(|(a, ctx)| sum_v2(a, ctx));
}
