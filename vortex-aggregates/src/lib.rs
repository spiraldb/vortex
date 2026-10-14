// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The built-in aggregate functions, built on the `vortex_array::aggregate_fn` framework.
//!
//! This includes the aggregates that back array statistics (min/max, sum, null and NaN counts,
//! sortedness, constancy, uncompressed size and the `all_*` predicates), the general-purpose
//! [`count`], [`first`], [`last`] and [`mean`], the byte-bounded [`bounded_min`]/[`bounded_max`]
//! used by zone maps, and the [`combined`] adapter that composes two aggregates into one.
//!
//! Array statistics resolve their aggregate through the session's registry, so call
//! [`initialize`] on any session that computes statistics, or start from [`session`].

use vortex_array::VTable;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::AggregateFnVTable;
use vortex_array::aggregate_fn::AggregateFnVTableExt;
use vortex_array::aggregate_fn::EmptyOptions;
use vortex_array::aggregate_fn::NumericalAggregateOpts;
use vortex_array::aggregate_fn::session::AggregateFnSession;
use vortex_array::aggregate_fn::session::AggregateFnSessionExt;
use vortex_array::aggregate_fn::stat_aggregates::UncompressedSizeOpts;
use vortex_array::arrays::Dict;
use vortex_array::arrays::Primitive;
use vortex_array::dtype::session::DTypeSession;
use vortex_array::expr::stats::Stat;
use vortex_array::memory::MemorySession;
use vortex_array::optimizer::kernels::KernelSession;
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_session::VortexSession;

pub mod all_nan;
pub mod all_non_distinct;
pub mod all_non_nan;
pub mod all_non_null;
pub mod all_null;
pub mod bounded_max;
pub mod bounded_min;
pub mod combined;
pub mod count;
mod dict;
pub mod first;
pub mod is_constant;
pub mod is_sorted;
pub mod last;
pub mod max;
pub mod mean;
pub mod min;
pub mod min_max;
pub mod nan_count;
pub mod null_count;
pub mod sum;
pub mod sum_v2;
pub mod uncompressed_size_in_bytes;

#[cfg(test)]
mod combined_tests;
#[cfg(test)]
mod framework_tests;

/// Builds a fresh [`VortexSession`] with vortex-array's built-in session variables and this crate's
/// aggregate functions registered.
///
/// The session holds arrays, dtypes, scalar functions, optimizer kernels, aggregate functions and
/// memory, so it can execute arrays and compute their statistics. Each call returns an independent
/// session, so callers may register further encodings or kernels into it. File, layout and runtime
/// state live in higher-level crates.
pub fn session() -> VortexSession {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<KernelSession>()
        .with::<DTypeSession>()
        .with::<ScalarFnSession>()
        .with::<AggregateFnSession>()
        .with::<MemorySession>();
    initialize(&session);
    session
}

/// Register this crate's aggregate functions and kernels with the given session.
pub fn initialize(session: &VortexSession) {
    let aggregate_fns = session.aggregate_fns();

    aggregate_fns.register(all_non_distinct::AllNonDistinct);
    aggregate_fns.register(all_non_nan::AllNonNan);
    aggregate_fns.register(all_non_null::AllNonNull);
    aggregate_fns.register(all_nan::AllNan);
    aggregate_fns.register(all_null::AllNull);
    aggregate_fns.register(bounded_max::BoundedMax);
    aggregate_fns.register(bounded_min::BoundedMin);
    aggregate_fns.register(first::First);
    aggregate_fns.register(is_constant::IsConstant);
    aggregate_fns.register(is_sorted::IsSorted);
    aggregate_fns.register(last::Last);
    aggregate_fns.register(max::Max);
    aggregate_fns.register(min::Min);
    aggregate_fns.register(min_max::MinMax);
    aggregate_fns.register(nan_count::NanCount);
    aggregate_fns.register(null_count::NullCount);
    aggregate_fns.register(sum::Sum);
    aggregate_fns.register(sum_v2::SumV2);
    aggregate_fns.register(uncompressed_size_in_bytes::UncompressedSizeInBytes);

    aggregate_fns.register_aggregate_kernel(
        Dict.id(),
        Some(min_max::MinMax.id()),
        &dict::min_max::DictMinMaxKernel,
    );
    aggregate_fns.register_aggregate_kernel(
        Dict.id(),
        Some(is_constant::IsConstant.id()),
        &dict::is_constant::DictIsConstantKernel,
    );
    aggregate_fns.register_aggregate_kernel(
        Dict.id(),
        Some(is_sorted::IsSorted.id()),
        &dict::is_sorted::DictIsSortedKernel,
    );

    aggregate_fns.register_grouped_kernel(count::Count.id(), &count::CountGroupedKernel);
    aggregate_fns.register_grouped_encoding_kernel(
        Primitive.id(),
        sum::Sum.id(),
        &sum::PrimitiveGroupedSumEncodingKernel,
    );
    aggregate_fns.register_grouped_encoding_kernel(
        Primitive.id(),
        sum_v2::SumV2.id(),
        &sum_v2::PrimitiveGroupedSumV2EncodingKernel,
    );
}

/// Return the aggregate function backing `stat`, if the statistic has a stat slot.
///
/// Statistics follow NaN-skipping semantics, and the uncompressed size statistic is exact.
/// Sortedness and constancy have no stat slot, so they return `None`.
pub fn stat_aggregate_fn(stat: Stat) -> Option<AggregateFnRef> {
    Some(match stat {
        Stat::Max => max::Max.bind(NumericalAggregateOpts::skip_nans()),
        Stat::Min => min::Min.bind(NumericalAggregateOpts::skip_nans()),
        Stat::Sum => sum::Sum.bind(NumericalAggregateOpts::skip_nans()),
        Stat::NullCount => null_count::NullCount.bind(EmptyOptions),
        Stat::NaNCount => nan_count::NanCount.bind(EmptyOptions),
        Stat::UncompressedSizeInBytes => {
            uncompressed_size_in_bytes::UncompressedSizeInBytes.bind(UncompressedSizeOpts::exact())
        }
        Stat::IsConstant | Stat::IsSorted | Stat::IsStrictSorted => return None,
    })
}
