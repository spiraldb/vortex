// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::unwrap_used)]

use std::sync::LazyLock;

use divan::Bencher;
use vortex_array::ArrayRef;
use vortex_array::ArrayVTable;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::AggregateFnVTable;
use vortex_array::aggregate_fn::DynGroupedAccumulator;
use vortex_array::aggregate_fn::GroupedAccumulator;
use vortex_array::aggregate_fn::GroupedArray;
use vortex_array::aggregate_fn::NumericalAggregateOpts;
use vortex_array::aggregate_fn::fns::sum_v2::SumV2;
use vortex_array::aggregate_fn::fns::sum_v2::sum_v2;
use vortex_array::aggregate_fn::kernels::DynAggregateKernel;
use vortex_array::aggregate_fn::kernels::DynGroupedAggregateKernel;
use vortex_array::aggregate_fn::session::AggregateFnSessionExt;
use vortex_array::arrays::FixedSizeListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_runend::RunEnd;
use vortex_session::VortexSession;

// Keep the one-element-group fallback below 1 ms in CodSpeed simulation.
const LEN: usize = 2_048;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    vortex_runend::initialize(&session);
    session
});

static FALLBACK_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    vortex_runend::initialize(&session);
    session
        .aggregate_fns()
        .register_aggregate_kernel(RunEnd.id(), Some(SumV2.id()), &Decline);
    session
        .aggregate_fns()
        .register_grouped_encoding_kernel(RunEnd.id(), SumV2.id(), &Decline);
    session
});

/// Keep the pre-specialization dispatch paths available for benchmark comparisons.
#[derive(Debug)]
struct Decline;

impl DynAggregateKernel for Decline {
    fn aggregate(
        &self,
        _aggregate_fn: &AggregateFnRef,
        _batch: &ArrayRef,
        _ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<Scalar>> {
        Ok(None)
    }
}

impl DynGroupedAggregateKernel for Decline {
    fn grouped_aggregate(
        &self,
        _aggregate_fn: &AggregateFnRef,
        _groups: &GroupedArray,
        _ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        Ok(None)
    }
}

fn main() {
    LazyLock::force(&SESSION);
    LazyLock::force(&FALLBACK_SESSION);
    divan::main();
}

fn runend(run_length: usize) -> ArrayRef {
    let ends =
        PrimitiveArray::from_iter((run_length..=LEN).step_by(run_length).map(|end| end as u64));
    let values = PrimitiveArray::from_option_iter(
        (0..ends.len())
            .map(|index| (index % 5 != 0).then_some(i32::try_from(index % 100).unwrap())),
    );
    RunEnd::try_new(
        ends.into_array(),
        values.into_array(),
        &mut SESSION.create_execution_ctx(),
    )
    .unwrap()
    .into_array()
}

fn bench_sum(bencher: Bencher, run_length: usize, session: &VortexSession) {
    let array = runend(run_length);
    bencher
        .with_inputs(|| session.create_execution_ctx())
        .bench_refs(|ctx| sum_v2(&array, ctx).unwrap());
}

fn bench_grouped(bencher: Bencher, elements: ArrayRef, group_size: u32, session: &VortexSession) {
    let dtype = elements.dtype().clone();
    let groups = FixedSizeListArray::try_new(
        elements,
        group_size,
        Validity::NonNullable,
        LEN / group_size as usize,
    )
    .unwrap()
    .into_array();
    bencher
        .with_inputs(|| {
            (
                GroupedAccumulator::try_new(
                    SumV2,
                    NumericalAggregateOpts::default(),
                    dtype.clone(),
                )
                .unwrap(),
                session.create_execution_ctx(),
            )
        })
        .bench_refs(|(acc, ctx)| {
            acc.accumulate_list(&groups, ctx).unwrap();
            acc.finish()
                .unwrap()
                .execute::<PrimitiveArray>(ctx)
                .unwrap()
        });
}

#[divan::bench(args = [4, 64, 1024])]
fn sum_runend(bencher: Bencher, run_length: usize) {
    bench_sum(bencher, run_length, &SESSION);
}

#[divan::bench(args = [4, 64, 1024])]
fn sum_runend_fallback(bencher: Bencher, run_length: usize) {
    bench_sum(bencher, run_length, &FALLBACK_SESSION);
}

#[divan::bench(args = [Validity::NonNullable, Validity::AllValid, Validity::AllInvalid])]
fn sum_runend_validity(bencher: Bencher, validity: &Validity) {
    let ends = PrimitiveArray::from_iter((64..=LEN).step_by(64).map(|end| end as u64));
    let values = PrimitiveArray::new(
        (0..ends.len())
            .map(|index| i32::try_from(index).unwrap())
            .collect::<Buffer<_>>(),
        validity.clone(),
    );
    let array = RunEnd::try_new(
        ends.into_array(),
        values.into_array(),
        &mut SESSION.create_execution_ctx(),
    )
    .unwrap()
    .into_array();

    bencher
        .with_inputs(|| SESSION.create_execution_ctx())
        .bench_refs(|ctx| sum_v2(&array, ctx).unwrap());
}

#[divan::bench(args = [4, 64, 1024], consts = [1, 2, 8, 128])]
fn grouped_runend<const GROUP_SIZE: u32>(bencher: Bencher, run_length: usize) {
    bench_grouped(bencher, runend(run_length), GROUP_SIZE, &SESSION);
}

#[divan::bench(args = [4, 64, 1024], consts = [1, 2, 8, 128])]
fn grouped_runend_fallback<const GROUP_SIZE: u32>(bencher: Bencher, run_length: usize) {
    bench_grouped(bencher, runend(run_length), GROUP_SIZE, &FALLBACK_SESSION);
}
