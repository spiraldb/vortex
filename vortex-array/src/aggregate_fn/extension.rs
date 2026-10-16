// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Generic adapter that defines an aggregate over one extension type by running an existing
//! aggregate over the extension's storage.

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
use crate::dtype::extension::ExtId;
use crate::dtype::extension::ExtVTable;
use crate::scalar::Scalar;

/// The aggregate `A` applied to one extension type through its storage.
///
/// Inputs must have an extension dtype of that type. Every batch is unwrapped to its storage array
/// and accumulated by `A`, so storage-encoding kernels registered for `A` still fire. Partials and
/// results are those of `A` over the storage dtype.
///
/// The adapter is an aggregate of its own, with id `<A>[<E>]`, so supporting a new pair is one
/// registration:
///
/// ```
/// # use vortex_array::aggregate_fn::ExtensionAggregate;
/// # use vortex_array::aggregate_fn::fns::sum::Sum;
/// # use vortex_array::aggregate_fn::session::AggregateFnSessionExt;
/// # use vortex_array::extension::datetime::Timestamp;
/// # let session = vortex_array::array_session();
/// session.aggregate_fns().register(ExtensionAggregate::new(Sum, Timestamp));
/// ```
///
/// Only register a pair when `A` over the storage is meaningful for the extension type, e.g. when
/// it orders and compares like its storage.
///
/// Only the aggregate is generic: the adapter implements [`AggregateFnVTable`] with `A`'s options
/// and partials, while the extension type is identified by its [`ExtId`] alone, so aggregates can
/// build the adapter for whichever extension dtype they are given.
#[derive(Clone, Debug)]
pub struct ExtensionAggregate<A: AggregateFnVTable> {
    aggregate: A,
    ext_id: ExtId,
    id: AggregateFnId,
}

impl<A: AggregateFnVTable> ExtensionAggregate<A> {
    /// Define `aggregate` over extension type `ext`.
    pub fn new(aggregate: A, ext: impl ExtVTable) -> Self {
        Self::for_ext_id(aggregate, ext.id())
    }

    /// Define `aggregate` over the extension type with id `ext_id`.
    pub fn for_ext_id(aggregate: A, ext_id: ExtId) -> Self {
        #[expect(
            clippy::disallowed_methods,
            reason = "the id is built from the aggregate and extension ids"
        )]
        let id = AggregateFnId::new(&format!("{}[{ext_id}]", aggregate.id()));
        Self {
            aggregate,
            ext_id,
            id,
        }
    }

    /// The storage dtype of `dtype`, if it is an extension dtype of this type.
    fn storage_dtype<'a>(&self, dtype: &'a DType) -> Option<&'a DType> {
        dtype
            .as_extension_opt()
            .filter(|ext_dtype| ext_dtype.id() == self.ext_id)
            .map(|ext_dtype| ext_dtype.storage_dtype())
    }

    /// Derive the storage aggregate's dtypes from this aggregate's, which share the partial and
    /// return dtypes.
    fn storage_dtypes(&self, args: AggregateArgs<'_, A::Options>) -> VortexResult<AggregateDTypes> {
        let storage_dtype = self.storage_dtype(args.dtype).ok_or_else(|| {
            vortex_err!(
                "Aggregate function {} expects extension type {}, got {}",
                self.id,
                self.ext_id,
                args.dtype
            )
        })?;
        Ok(AggregateDTypes::new(
            storage_dtype.clone(),
            args.return_dtype.clone(),
            args.partial_dtype.clone(),
        ))
    }
}

impl<A: AggregateFnVTable> AggregateFnVTable for ExtensionAggregate<A> {
    type Options = A::Options;
    // The storage aggregate is held as a fully-fledged `Accumulator` so that storage batches
    // consult the kernel registry and the cached statistics of `A`.
    type Partial = Accumulator<A>;

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
        Ok(Accumulator::from_dtypes(
            self.aggregate.clone(),
            args.options.clone(),
            self.storage_dtypes(args)?,
        ))
    }

    fn partial_from_scalar(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        scalar: &Scalar,
    ) -> VortexResult<Self::Partial> {
        let mut partial = self.empty_partial(args)?;
        partial.combine_partials(scalar)?;
        Ok(partial)
    }

    fn merge_partials(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
        mut first: Self::Partial,
        mut second: Self::Partial,
    ) -> VortexResult<Self::Partial> {
        first.merge_from(&mut second)?;
        Ok(first)
    }

    fn to_scalar(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
        partial: &Self::Partial,
    ) -> VortexResult<Scalar> {
        partial.partial_scalar()
    }

    fn is_saturated(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
        partial: &Self::Partial,
    ) -> bool {
        partial.is_saturated()
    }

    /// Unwrap extension arrays to their storage before decoding, so the storage accumulator sees
    /// the storage encoding.
    fn try_accumulate(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
        state: &mut Self::Partial,
        batch: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<bool> {
        let Some(array) = batch.as_opt::<Extension>() else {
            return Ok(false);
        };
        state.accumulate(array.storage_array(), ctx)?;
        Ok(true)
    }

    fn accumulate(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
        state: &mut Self::Partial,
        batch: &Columnar,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        match batch {
            Columnar::Canonical(Canonical::Extension(array)) => {
                state.accumulate(array.storage_array(), ctx)
            }
            Columnar::Constant(constant) => {
                let storage = constant.scalar().as_extension().to_storage_scalar();
                state.accumulate(
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
        let dtypes = self.storage_dtypes(args)?;
        self.aggregate.finalize(dtypes.args(args.options), states)
    }

    fn finalize_scalar(
        &self,
        _args: AggregateArgs<'_, Self::Options>,
        partial: &Self::Partial,
    ) -> VortexResult<Scalar> {
        partial.final_scalar()
    }
}

#[cfg(test)]
mod tests {
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;

    use super::ExtensionAggregate;
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::aggregate_fn::Accumulator;
    use crate::aggregate_fn::AggregateFnVTable;
    use crate::aggregate_fn::DynAccumulator;
    use crate::aggregate_fn::NumericalAggregateOpts;
    use crate::aggregate_fn::fns::min_max::MinMax;
    use crate::aggregate_fn::fns::min_max::MinMaxResult;
    use crate::aggregate_fn::fns::min_max::min_max;
    use crate::aggregate_fn::fns::sum::Sum;
    use crate::aggregate_fn::session::AggregateFnSessionExt;
    use crate::arrays::ConstantArray;
    use crate::arrays::ExtensionArray;
    use crate::arrays::PrimitiveArray;
    use crate::dtype::DType;
    use crate::dtype::Nullability;
    use crate::dtype::PType;
    use crate::dtype::extension::ExtDType;
    use crate::extension::datetime::Date;
    use crate::extension::datetime::TimeUnit;
    use crate::extension::datetime::Timestamp;
    use crate::extension::tests::divisible_int::DivisibleInt;
    use crate::extension::tests::divisible_int::Divisor;
    use crate::scalar::Scalar;

    #[test]
    fn sums_extension_through_storage() -> VortexResult<()> {
        let session = crate::array_session();
        let aggregate = ExtensionAggregate::new(Sum, Timestamp);
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
            Sum,
            NumericalAggregateOpts::default(),
            storage.dtype().clone(),
        )?;
        expected.accumulate(&storage, &mut ctx)?;

        assert_eq!(acc.finish()?, expected.finish()?);
        Ok(())
    }

    /// Registering `ExtensionAggregate<MinMax>` gives an extension type extrema, as extension
    /// scalars.
    #[test]
    fn registered_min_max_gives_extension_extrema() -> VortexResult<()> {
        let session = crate::array_session();
        let mut ctx = session.create_execution_ctx();
        let opts = NumericalAggregateOpts::default();

        let ext_dtype = ExtDType::<DivisibleInt>::try_new(
            Divisor(5),
            DType::Primitive(PType::U64, Nullability::NonNullable),
        )?
        .erased();
        let ext_scalar = |value: u64| Scalar::extension_ref(ext_dtype.clone(), Scalar::from(value));
        let array =
            ExtensionArray::new(ext_dtype.clone(), buffer![15u64, 5, 10].into_array()).into_array();
        let constant = ConstantArray::new(ext_scalar(20), 2).into_array();

        assert_eq!(min_max(&array, &mut ctx, opts)?, None);
        assert_eq!(min_max(&constant, &mut ctx, opts)?, None);

        session
            .aggregate_fns()
            .register(ExtensionAggregate::new(MinMax, DivisibleInt));
        assert_eq!(
            min_max(&array, &mut ctx, opts)?,
            Some(MinMaxResult {
                min: ext_scalar(5),
                max: ext_scalar(15),
            })
        );
        assert_eq!(
            min_max(&constant, &mut ctx, opts)?,
            Some(MinMaxResult {
                min: ext_scalar(20),
                max: ext_scalar(20),
            })
        );
        Ok(())
    }

    #[test]
    fn rejects_other_extension_types() {
        let date = DType::Extension(Date::new(TimeUnit::Days, Nullability::Nullable).erased());
        assert!(
            ExtensionAggregate::new(Sum, Timestamp)
                .return_dtype(&NumericalAggregateOpts::default(), &date)
                .is_none()
        );
    }
}
