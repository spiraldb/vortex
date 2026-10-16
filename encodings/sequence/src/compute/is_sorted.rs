// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use num_traits::zero;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::fns::is_sorted::IsSorted;
use vortex_array::aggregate_fn::kernels::DynAggregateKernel;
use vortex_array::match_each_native_ptype;
use vortex_array::scalar::Scalar;
use vortex_error::VortexResult;

use crate::Sequence;

/// Sequence-specific is_sorted kernel.
///
/// A sequence `A[i] = base + i * multiplier` is sorted iff multiplier >= 0,
/// and strict sorted iff multiplier > 0.
#[derive(Debug)]
pub(crate) struct SequenceIsSortedKernel;

impl DynAggregateKernel for SequenceIsSortedKernel {
    fn aggregate(
        &self,
        aggregate_fn: &AggregateFnRef,
        batch: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<Scalar>> {
        let Some(options) = aggregate_fn.as_opt::<IsSorted>() else {
            return Ok(None);
        };

        let Some(array) = batch.as_opt::<Sequence>() else {
            return Ok(None);
        };

        let m = array.multiplier();
        let result = match_each_native_ptype!(m.ptype(), |P| {
            m.cast::<P>().map(|x| {
                if options.strict {
                    x > zero::<P>()
                } else {
                    x >= zero::<P>()
                }
            })
        })?;

        // At most one element is sorted under either ordering, whatever the multiplier.
        let result = result || batch.len() <= 1;

        Ok(Some(IsSorted::make_partial(
            batch,
            result,
            options.strict,
            ctx,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use rstest::rstest;
    use vortex_array::ArrayRef;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::aggregate_fn::AggregateFn;
    use vortex_array::aggregate_fn::fns::is_sorted::IsSorted;
    use vortex_array::aggregate_fn::fns::is_sorted::IsSortedOptions;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::expr::stats::Precision;
    use vortex_array::expr::stats::Stat;
    use vortex_array::expr::stats::StatsProviderExt;
    use vortex_array::scalar::PValue;
    use vortex_array::scalar::Scalar;
    use vortex_error::VortexResult;
    use vortex_session::VortexSession;

    use crate::Sequence;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
        let session = vortex_array::array_session();
        crate::initialize(&session);
        session
    });

    /// One element of a sequence with a zero multiplier.
    fn single_element() -> VortexResult<ArrayRef> {
        Sequence::try_new(
            PValue::from(113i8),
            PValue::from(0i8),
            PType::I8,
            Nullability::NonNullable,
            30,
        )?
        .into_array()
        .slice(4..5)
    }

    #[test]
    fn single_element_stats_are_strict_sorted() -> VortexResult<()> {
        let array = single_element()?;
        let stats = array.statistics();
        for stat in [Stat::IsSorted, Stat::IsStrictSorted] {
            assert_eq!(
                stats.with_typed_stats_set(|s| s.get_as::<bool>(stat)),
                Precision::Exact(true)
            );
        }
        Ok(())
    }

    #[rstest]
    fn single_element_aggregate_is_sorted(#[values(false, true)] strict: bool) -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let array = single_element()?;
        let mut acc = AggregateFn::new(IsSorted, IsSortedOptions { strict })
            .erased()
            .accumulator(array.dtype())?;

        acc.accumulate(&array, &mut ctx)?;
        assert_eq!(
            acc.final_scalar()?,
            Scalar::bool(true, Nullability::NonNullable)
        );
        Ok(())
    }
}
