// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Opt-in pushdown of an aggregate into the storage of one extension type.
//!
//! See [`StoragePushdown`] for when such a pushdown is correct.

use std::mem;

use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_session::VortexSession;

use crate::ArrayRef;
use crate::Canonical;
use crate::Columnar;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::aggregate_fn::Accumulator;
use crate::aggregate_fn::AggregateArgs;
use crate::aggregate_fn::AggregateDTypes;
use crate::aggregate_fn::AggregateFnId;
use crate::aggregate_fn::AggregateFnVTable;
use crate::aggregate_fn::DynAccumulator;
use crate::arrays::ConstantArray;
use crate::arrays::Extension;
use crate::arrays::extension::ExtensionArrayExt;
use crate::dtype::DType;
use crate::dtype::extension::ExtVTable;
use crate::scalar::Scalar;

/// An opt-in pushdown of aggregate `A` into the storage of extension type `E`.
///
/// An extension type gives its storage a meaning: a [`Timestamp`] is an instant, not an `i64`.
/// Aggregates do not see through extension types on their own, so most of them reject an
/// extension dtype. `StoragePushdown` lets you declare, for one aggregate and one extension type
/// at a time, that running the aggregate on the storage gives the right answer for the extension
/// type.
///
/// **Registering a pair is a claim about `E`, and nothing checks it.** The adapter runs `A` on
/// the raw storage values whether or not that is meaningful for `E`. A wrong registration does
/// not fail. It silently returns answers that are wrong for `E`, and query planning and pruning
/// may rely on those answers.
///
/// # When a pushdown is correct
///
/// Every aggregate is built from a few operations on its input values: comparison, equality,
/// addition, or NaN detection. A pushdown of `A` into `E`'s storage is correct only when each
/// operation `A` uses can be derived from the same operation on `E`'s storage. That is a
/// property of the specific type `E` and the specific aggregate `A`, not of extension types in
/// general. One extension type can support pushdown for some aggregates and not others.
///
/// [`Timestamp`] shows both cases:
///
/// - Comparing two timestamps of the same unit and time zone is the same as comparing their `i64`
///   storage. So `min`, `max` and `is_sorted` can be pushed down into its storage.
/// - Adding two timestamps is not meaningful: the sum of two instants is not an instant. The `i64`
///   addition on the storage is still defined, though, so a `sum` pushdown would run and return a
///   meaningless number. `sum` must not be registered for `Timestamp`.
///
/// Check each aggregate against this table before registering it for a type:
///
/// | Aggregates | Operation used | Holds for `E` when... | Fits | Does not fit |
/// | --- | --- | --- | --- | --- |
/// | `min`, `max`, `min_max`, `bounded_min`, `bounded_max`, `is_sorted` | comparison | comparing two values of `E` is comparing their storage | `Date`, `Time`, `Timestamp` | a case-insensitive string type, a little-endian integer stored as bytes |
/// | `is_constant`, `all_non_distinct` | equality | each value of `E` has exactly one storage value, so equal values have equal storage | `Date`, `Time`, `Timestamp`, `Uuid` | JSON text (`{"a":1}` and `{ "a": 1 }` are equal), a case-insensitive string type |
/// | `sum`, `mean` | addition | adding the storage of two values gives the storage of their sum | a duration stored as an integer count | `Timestamp`, `Date`, `Uuid` |
/// | `nan_count`, `all_nan`, `all_non_nan` | NaN detection | a NaN storage value is a NaN value of `E` | a float-backed measurement type | a type that stores a sentinel in its float storage |
/// | `count`, `null_count`, `all_null`, `all_non_null`, `uncompressed_size_in_bytes` | none: validity or storage bytes only | always | any `E` | |
///
/// If an aggregate is not in the table, work out which operations it uses and check each one.
///
/// # Behavior
///
/// - Inputs must have an extension dtype of type `E`. For any other input, `return_dtype` and
///   `partial_dtype` return `None`, so binding fails.
/// - `A` must also accept `E`'s storage dtype. If it does not, the pair still registers but
///   binding it to a column of `E` fails. For example, `IsSorted` rejects the fixed-size-list
///   storage of `Uuid`.
/// - Each batch is unwrapped to its storage array and accumulated by `A`, so kernels and cached
///   statistics of `A` on the storage encoding still apply.
/// - Partials and results are those of `A` over the storage dtype, and are not converted back to
///   `E`. For example, the extrema of a timestamp column come back as raw `i64` storage values,
///   in the column's unit.
/// - Partials depend on `E`'s metadata as well as on `E`. Timestamps in milliseconds and in
///   seconds share the `Timestamp` type but not the scale of their storage. Only merge partials
///   accumulated over the same extension dtype.
/// - The pushdown is an aggregate of its own, with id `<A>[<E>]` (e.g.
///   `vortex.is_sorted[vortex.timestamp]`). It never replaces `A`, and a request for `A` on a
///   column of `E` does not use it. Its options are `A`'s options, and it serializes through
///   `A`, so a registered pair round-trips by id.
///
/// [`Timestamp`]: crate::extension::datetime::Timestamp
///
/// # Example
///
/// Timestamps compare like their `i64` storage, so `is_sorted` can be pushed down into the
/// storage. Register the pair once on the session, then bind it like any other aggregate:
///
/// ```
/// # use vortex_array::IntoArray;
/// # use vortex_array::VortexSessionExecute;
/// # use vortex_array::aggregate_fn::Accumulator;
/// # use vortex_array::aggregate_fn::DynAccumulator;
/// # use vortex_array::aggregate_fn::StoragePushdown;
/// # use vortex_array::aggregate_fn::fns::is_sorted::IsSorted;
/// # use vortex_array::aggregate_fn::fns::is_sorted::IsSortedOptions;
/// # use vortex_array::aggregate_fn::session::AggregateFnSessionExt;
/// # use vortex_array::arrays::ExtensionArray;
/// # use vortex_array::arrays::PrimitiveArray;
/// # use vortex_array::dtype::DType;
/// # use vortex_array::dtype::Nullability;
/// # use vortex_array::extension::datetime::TimeUnit;
/// # use vortex_array::extension::datetime::Timestamp;
/// # fn main() -> vortex_error::VortexResult<()> {
/// let session = vortex_array::array_session();
///
/// // Register `vortex.is_sorted[vortex.timestamp]`.
/// let is_sorted = StoragePushdown::new(IsSorted, Timestamp);
/// session.aggregate_fns().register(is_sorted.clone());
///
/// // Aggregate a timestamp column through its storage.
/// let ext_dtype = Timestamp::new(TimeUnit::Milliseconds, Nullability::NonNullable).erased();
/// let column = ExtensionArray::new(
///     ext_dtype.clone(),
///     PrimitiveArray::from_iter([1_000i64, 2_000, 3_000]).into_array(),
/// )
/// .into_array();
///
/// let mut acc = Accumulator::try_new(
///     is_sorted,
///     IsSortedOptions { strict: true },
///     DType::Extension(ext_dtype),
/// )?;
/// acc.accumulate(&column, &mut session.create_execution_ctx())?;
/// assert!(bool::try_from(&acc.finish()?)?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct StoragePushdown<A: AggregateFnVTable, E: ExtVTable> {
    aggregate: A,
    ext: E,
    id: AggregateFnId,
}

impl<A: AggregateFnVTable, E: ExtVTable> StoragePushdown<A, E> {
    /// Push `aggregate` down into the storage of extension type `ext`.
    ///
    /// Only do this when `aggregate` over the storage is correct for `ext`; see
    /// [`StoragePushdown`] for how to check.
    pub fn new(aggregate: A, ext: E) -> Self {
        #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
        let id = AggregateFnId::new(&format!("{}[{}]", aggregate.id(), ext.id()));
        Self { aggregate, ext, id }
    }

    /// The storage dtype of `dtype`, if it is an extension dtype of type `E`.
    fn storage_dtype<'a>(&self, dtype: &'a DType) -> Option<&'a DType> {
        dtype
            .as_extension_opt()
            .filter(|ext_dtype| ext_dtype.is::<E>())
            .map(|ext_dtype| ext_dtype.storage_dtype())
    }

    /// The arguments of the storage aggregate `A`.
    ///
    /// `A` shares this aggregate's options, return and partial dtypes, and its input dtype is
    /// borrowed from the extension dtype, so this allocates nothing.
    fn storage_args<'a>(
        &self,
        args: AggregateArgs<'a, A::Options>,
    ) -> VortexResult<AggregateArgs<'a, A::Options>> {
        let dtype = self.storage_dtype(args.dtype).ok_or_else(|| {
            vortex_err!(
                "Aggregate function {} expects extension type {}, got {}",
                self.id,
                self.ext.id(),
                args.dtype
            )
        })?;
        Ok(AggregateArgs { dtype, ..args })
    }

    /// Accumulate a storage batch into `state` through an [`Accumulator`] of `A`, so that the
    /// kernels and cached statistics of `A` on the storage encoding still apply.
    fn accumulate_storage(
        &self,
        args: AggregateArgs<'_, A::Options>,
        state: &mut A::Partial,
        storage: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        let mut accumulator = Accumulator::from_dtypes(
            self.aggregate.clone(),
            args.options.clone(),
            AggregateDTypes::new(
                args.dtype.clone(),
                args.return_dtype.clone(),
                args.partial_dtype.clone(),
            ),
        );
        accumulator.accumulate(storage, ctx)?;
        let Some(batch) = accumulator.take_partial() else {
            return Ok(());
        };
        let running = mem::replace(state, self.aggregate.empty_partial(args)?);
        *state = self.aggregate.merge_partials(args, running, batch)?;
        Ok(())
    }
}

impl<A: AggregateFnVTable, E: ExtVTable> AggregateFnVTable for StoragePushdown<A, E> {
    type Options = A::Options;
    type Partial = A::Partial;

    fn id(&self) -> AggregateFnId {
        self.id
    }

    fn serialize(&self, options: &Self::Options) -> VortexResult<Option<Vec<u8>>> {
        self.aggregate.serialize(options)
    }

    fn deserialize(&self, metadata: &[u8], session: &VortexSession) -> VortexResult<Self::Options> {
        self.aggregate.deserialize(metadata, session)
    }

    fn return_dtype(&self, options: &Self::Options, input_dtype: &DType) -> Option<DType> {
        self.aggregate
            .return_dtype(options, self.storage_dtype(input_dtype)?)
    }

    fn partial_dtype(&self, options: &Self::Options, input_dtype: &DType) -> Option<DType> {
        self.aggregate
            .partial_dtype(options, self.storage_dtype(input_dtype)?)
    }

    fn empty_partial(&self, args: AggregateArgs<'_, Self::Options>) -> VortexResult<Self::Partial> {
        self.aggregate.empty_partial(self.storage_args(args)?)
    }

    fn partial_from_scalar(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        scalar: &Scalar,
    ) -> VortexResult<Self::Partial> {
        self.aggregate
            .partial_from_scalar(self.storage_args(args)?, scalar)
    }

    fn merge_partials(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        first: Self::Partial,
        second: Self::Partial,
    ) -> VortexResult<Self::Partial> {
        self.aggregate
            .merge_partials(self.storage_args(args)?, first, second)
    }

    fn to_scalar(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        partial: &Self::Partial,
    ) -> VortexResult<Scalar> {
        self.aggregate.to_scalar(self.storage_args(args)?, partial)
    }

    fn is_saturated(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        partial: &Self::Partial,
    ) -> bool {
        self.storage_args(args)
            .is_ok_and(|args| self.aggregate.is_saturated(args, partial))
    }

    /// Unwrap extension arrays to their storage before decoding, so the storage accumulator sees
    /// the storage encoding.
    fn try_accumulate(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        state: &mut Self::Partial,
        batch: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<bool> {
        let Some(array) = batch.as_opt::<Extension>() else {
            return Ok(false);
        };
        self.accumulate_storage(self.storage_args(args)?, state, array.storage_array(), ctx)?;
        Ok(true)
    }

    fn accumulate(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        state: &mut Self::Partial,
        batch: &Columnar,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        let args = self.storage_args(args)?;
        match batch {
            Columnar::Canonical(Canonical::Extension(array)) => {
                self.accumulate_storage(args, state, array.storage_array(), ctx)
            }
            Columnar::Constant(constant) => {
                let storage = constant.scalar().as_extension().to_storage_scalar();
                self.accumulate_storage(
                    args,
                    state,
                    &ConstantArray::new(storage, constant.len()).into_array(),
                    ctx,
                )
            }
            Columnar::Canonical(_) => {
                vortex_bail!(
                    "Aggregate function {} expects an extension array, got {}",
                    self.id,
                    batch.dtype()
                )
            }
        }
    }

    fn finalize(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        states: ArrayRef,
    ) -> VortexResult<ArrayRef> {
        self.aggregate.finalize(self.storage_args(args)?, states)
    }

    fn finalize_scalar(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        partial: &Self::Partial,
    ) -> VortexResult<Scalar> {
        self.aggregate
            .finalize_scalar(self.storage_args(args)?, partial)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_error::VortexResult;

    use super::StoragePushdown;
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::aggregate_fn::Accumulator;
    use crate::aggregate_fn::AggregateFnVTable;
    use crate::aggregate_fn::DynAccumulator;
    use crate::aggregate_fn::NumericalAggregateOpts;
    use crate::aggregate_fn::fns::is_sorted::IsSorted;
    use crate::aggregate_fn::fns::is_sorted::IsSortedOptions;
    use crate::aggregate_fn::fns::min_max::MinMax;
    use crate::aggregate_fn::session::AggregateFnSessionExt;
    use crate::arrays::ExtensionArray;
    use crate::arrays::PrimitiveArray;
    use crate::dtype::DType;
    use crate::dtype::Nullability;
    use crate::extension::datetime::Date;
    use crate::extension::datetime::Time;
    use crate::extension::datetime::TimeUnit;
    use crate::extension::datetime::Timestamp;

    #[test]
    fn pushes_down_into_storage() -> VortexResult<()> {
        let session = crate::array_session();
        let aggregate = StoragePushdown::new(MinMax, Timestamp);
        session.aggregate_fns().register(aggregate.clone());
        assert!(
            session
                .aggregate_fns()
                .find_plugin(&aggregate.id())
                .is_some()
        );

        let ext_dtype = Timestamp::new(TimeUnit::Milliseconds, Nullability::Nullable).erased();
        let storage =
            PrimitiveArray::from_option_iter([Some(3_000i64), None, Some(1_000)]).into_array();
        let array = ExtensionArray::new(ext_dtype.clone(), storage.clone()).into_array();

        let mut ctx = session.create_execution_ctx();
        let mut acc = Accumulator::try_new(
            aggregate,
            NumericalAggregateOpts::default(),
            DType::Extension(ext_dtype),
        )?;
        acc.accumulate(&array, &mut ctx)?;

        let mut expected = Accumulator::try_new(
            MinMax,
            NumericalAggregateOpts::default(),
            storage.dtype().clone(),
        )?;
        expected.accumulate(&storage, &mut ctx)?;

        assert_eq!(acc.finish()?, expected.finish()?);
        Ok(())
    }

    #[test]
    fn combines_flushed_partials() -> VortexResult<()> {
        let session = crate::array_session();
        let mut ctx = session.create_execution_ctx();
        let ext_dtype = Timestamp::new(TimeUnit::Milliseconds, Nullability::Nullable).erased();
        let batch = |values: [Option<i64>; 2]| {
            ExtensionArray::new(
                ext_dtype.clone(),
                PrimitiveArray::from_option_iter(values).into_array(),
            )
            .into_array()
        };
        let accumulator = || {
            Accumulator::try_new(
                StoragePushdown::new(MinMax, Timestamp),
                NumericalAggregateOpts::default(),
                DType::Extension(ext_dtype.clone()),
            )
        };

        let mut first = accumulator()?;
        first.accumulate(&batch([Some(3_000), None]), &mut ctx)?;
        let mut second = accumulator()?;
        second.accumulate(&batch([Some(1_000), Some(500)]), &mut ctx)?;
        first.combine_partials(&second.flush()?)?;

        let mut expected = accumulator()?;
        expected.accumulate(&batch([Some(3_000), None]), &mut ctx)?;
        expected.accumulate(&batch([Some(1_000), Some(500)]), &mut ctx)?;

        assert_eq!(first.finish()?, expected.finish()?);
        Ok(())
    }

    #[test]
    fn rejects_other_extension_types() {
        let date = DType::Extension(Date::new(TimeUnit::Days, Nullability::Nullable).erased());
        assert!(
            StoragePushdown::new(MinMax, Timestamp)
                .return_dtype(&NumericalAggregateOpts::default(), &date)
                .is_none()
        );
    }

    #[test]
    fn default_session_registers_temporal_order_aggregates() {
        let session = crate::array_session();
        let aggregate_fns = session.aggregate_fns();
        for id in [
            StoragePushdown::new(IsSorted, Date).id(),
            StoragePushdown::new(IsSorted, Time).id(),
            StoragePushdown::new(IsSorted, Timestamp).id(),
            StoragePushdown::new(MinMax, Date).id(),
            StoragePushdown::new(MinMax, Time).id(),
            StoragePushdown::new(MinMax, Timestamp).id(),
        ] {
            assert!(
                aggregate_fns.find_plugin(&id).is_some(),
                "{id} is not registered"
            );
        }
    }

    #[rstest]
    // Nulls sort first.
    #[case::sorted([None, Some(1_000), Some(2_000), Some(2_000)], false, true)]
    #[case::unsorted([Some(2_000), Some(1_000), None, Some(3_000)], false, false)]
    #[case::strict_with_duplicates([None, Some(1_000), Some(2_000), Some(2_000)], true, false)]
    fn is_sorted_over_timestamps(
        #[case] values: [Option<i64>; 4],
        #[case] strict: bool,
        #[case] expected: bool,
    ) -> VortexResult<()> {
        let session = crate::array_session();
        let mut ctx = session.create_execution_ctx();
        let ext_dtype = Timestamp::new(TimeUnit::Milliseconds, Nullability::Nullable).erased();
        let array = ExtensionArray::new(
            ext_dtype.clone(),
            PrimitiveArray::from_option_iter(values).into_array(),
        )
        .into_array();

        let mut acc = Accumulator::try_new(
            StoragePushdown::new(IsSorted, Timestamp),
            IsSortedOptions { strict },
            DType::Extension(ext_dtype),
        )?;
        acc.accumulate(&array, &mut ctx)?;

        assert_eq!(bool::try_from(&acc.finish()?)?, expected);
        Ok(())
    }

    #[test]
    fn min_max_over_dates_matches_storage() -> VortexResult<()> {
        let session = crate::array_session();
        let mut ctx = session.create_execution_ctx();
        let ext_dtype = Date::new(TimeUnit::Days, Nullability::Nullable).erased();
        let storage =
            PrimitiveArray::from_option_iter([Some(20i32), None, Some(-3), Some(7)]).into_array();
        let array = ExtensionArray::new(ext_dtype.clone(), storage.clone()).into_array();

        let mut acc = Accumulator::try_new(
            StoragePushdown::new(MinMax, Date),
            NumericalAggregateOpts::default(),
            DType::Extension(ext_dtype),
        )?;
        acc.accumulate(&array, &mut ctx)?;

        let mut expected = Accumulator::try_new(
            MinMax,
            NumericalAggregateOpts::default(),
            storage.dtype().clone(),
        )?;
        expected.accumulate(&storage, &mut ctx)?;

        assert_eq!(acc.finish()?, expected.finish()?);
        Ok(())
    }
}
