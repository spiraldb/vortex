// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Generic adapter for aggregates over an extension dtype that are computed by running an
//! aggregate over the extension's storage dtype.

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
use crate::arrays::ExtensionArray;
use crate::arrays::extension::ExtensionArrayExt;
use crate::dtype::DType;
use crate::dtype::extension::ExtDTypeRef;
use crate::scalar::Scalar;

type StorageOptions<L> = <<L as StorageLifter>::Storage as AggregateFnVTable>::Options;

/// Declare an aggregate over an extension dtype in terms of an aggregate over its storage dtype.
///
/// Wrap an implementation in [`ExtensionDelegate`] to obtain an [`AggregateFnVTable`]. Every batch
/// is unwrapped to its storage array and accumulated by [`Self::Storage`], so storage-encoding
/// kernels registered for the storage aggregate still fire. Partial states are the storage
/// aggregate's partials; only the final result is lifted back with [`Self::lift_scalar`] and
/// [`Self::lift_array`].
///
/// The defaults return the storage result unchanged, which suits aggregates whose result does not
/// depend on the input dtype, e.g. `is_constant` or `is_sorted`.
pub trait StorageLifter: 'static + Send + Sync + Clone {
    /// The aggregate computed over the storage dtype.
    type Storage: AggregateFnVTable;

    /// Stable identifier of the lifted aggregate.
    ///
    /// It must differ from the storage aggregate's id: the lifted partials and results have
    /// different dtypes, so kernels and cached statistics of one cannot stand in for the other.
    fn id(&self) -> AggregateFnId;

    /// Construct the storage aggregate vtable.
    fn storage(&self) -> Self::Storage;

    /// Compute the final storage result over a whole storage array.
    ///
    /// Used by [`ExtensionDelegate::aggregate`]. Override it to reuse a storage entry point that
    /// consults cached statistics, e.g. `is_constant`.
    fn aggregate_storage(
        &self,
        options: &StorageOptions<Self>,
        storage: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let mut acc = Accumulator::try_new(self.storage(), options.clone(), storage.dtype().clone())?;
        acc.accumulate(storage, ctx)?;
        acc.finish()
    }

    /// The return dtype of the lifted aggregate, given the storage aggregate's return dtype.
    fn lift_return_dtype(&self, ext_dtype: &ExtDTypeRef, storage_dtype: DType) -> Option<DType> {
        let _ = ext_dtype;
        Some(storage_dtype)
    }

    /// Lift a storage result scalar into a result of the lifted return dtype.
    fn lift_scalar(&self, ext_dtype: &ExtDTypeRef, storage_result: Scalar) -> VortexResult<Scalar> {
        let _ = ext_dtype;
        Ok(storage_result)
    }

    /// Lift an array of storage results into an array of the lifted return dtype.
    fn lift_array(
        &self,
        ext_dtype: &ExtDTypeRef,
        storage_results: ArrayRef,
    ) -> VortexResult<ArrayRef> {
        let _ = ext_dtype;
        Ok(storage_results)
    }
}

/// Adapter that exposes any [`StorageLifter`] as an [`AggregateFnVTable`] over extension dtypes.
#[derive(Clone, Debug)]
pub struct ExtensionDelegate<L: StorageLifter>(pub L);

impl<L: StorageLifter> ExtensionDelegate<L> {
    /// Construct a new extension delegate vtable.
    pub fn new(lifter: L) -> Self {
        Self(lifter)
    }

    /// Aggregate an extension `array` into the lifted final result.
    ///
    /// This is the entry point for aggregates that handle extension inputs inside their own
    /// `accumulate` by delegating to the storage array.
    pub fn aggregate(
        &self,
        options: &StorageOptions<L>,
        array: &ExtensionArray,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let storage_result = self
            .0
            .aggregate_storage(options, array.storage_array(), ctx)?;
        self.0.lift_scalar(array.ext_dtype(), storage_result)
    }

    /// Accumulate an extension `array` straight into a partial state of the storage aggregate.
    ///
    /// This suits aggregates whose partial state does not depend on the input dtype, e.g. a Bloom
    /// filter, which can then fold extension batches into the partial they already hold.
    pub fn accumulate_storage(
        &self,
        options: &StorageOptions<L>,
        partial: &mut <L::Storage as AggregateFnVTable>::Partial,
        array: &ExtensionArray,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        let storage = self.0.storage();
        let dtypes =
            AggregateDTypes::try_new(&storage, options, array.storage_array().dtype().clone())?;
        let args = dtypes.args(options);
        if storage.try_accumulate(args, partial, array.storage_array(), ctx)? {
            return Ok(());
        }
        let columnar = array.storage_array().clone().execute::<Columnar>(ctx)?;
        storage.accumulate(args, partial, &columnar, ctx)
    }

    /// Derive the storage aggregate's dtypes from the lifted aggregate's.
    ///
    /// The partial dtypes are shared, so only the storage return dtype is left to resolve.
    fn storage_dtypes<'a>(
        &self,
        args: AggregateArgs<'a, StorageOptions<L>>,
    ) -> VortexResult<(&'a ExtDTypeRef, AggregateDTypes)> {
        let ext_dtype = ext_dtype(args.dtype)?;
        let storage = self.0.storage();
        let storage_dtype = ext_dtype.storage_dtype().clone();
        let return_dtype = storage
            .return_dtype(args.options, &storage_dtype)
            .ok_or_else(|| {
                vortex_err!(
                    "Aggregate function {} cannot be applied to dtype {}",
                    storage.id(),
                    storage_dtype
                )
            })?;
        Ok((
            ext_dtype,
            AggregateDTypes::new(storage_dtype, return_dtype, args.partial_dtype.clone()),
        ))
    }
}

fn ext_dtype(dtype: &DType) -> VortexResult<&ExtDTypeRef> {
    dtype
        .as_extension_opt()
        .ok_or_else(|| vortex_err!("Extension delegate expects an extension dtype, got {dtype}"))
}

impl<L: StorageLifter> AggregateFnVTable for ExtensionDelegate<L> {
    type Options = StorageOptions<L>;
    // The storage aggregate is held as a fully-fledged `Accumulator` so that storage batches
    // consult the kernel registry and the cached statistics of the storage aggregate.
    type Partial = Accumulator<L::Storage>;

    fn id(&self) -> AggregateFnId {
        self.0.id()
    }

    fn serialize(&self, options: &Self::Options) -> VortexResult<Option<Vec<u8>>> {
        self.0.storage().serialize(options)
    }

    fn deserialize(&self, metadata: &[u8], session: &VortexSession) -> VortexResult<Self::Options> {
        self.0.storage().deserialize(metadata, session)
    }

    fn return_dtype(&self, options: &Self::Options, input_dtype: &DType) -> Option<DType> {
        let ext_dtype = input_dtype.as_extension_opt()?;
        let storage_return = self
            .0
            .storage()
            .return_dtype(options, ext_dtype.storage_dtype())?;
        self.0.lift_return_dtype(ext_dtype, storage_return)
    }

    fn partial_dtype(&self, options: &Self::Options, input_dtype: &DType) -> Option<DType> {
        let ext_dtype = input_dtype.as_extension_opt()?;
        self.0
            .storage()
            .partial_dtype(options, ext_dtype.storage_dtype())
    }

    fn empty_partial(&self, args: AggregateArgs<'_, Self::Options>) -> VortexResult<Self::Partial> {
        let (_, dtypes) = self.storage_dtypes(args)?;
        Ok(Accumulator::from_dtypes(
            self.0.storage(),
            args.options.clone(),
            dtypes,
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
                state.accumulate(&ConstantArray::new(storage, constant.len()).into_array(), ctx)
            }
            Columnar::Canonical(_) => {
                vortex_bail!(
                    "Extension delegate expects an extension array, got {}",
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
        let (ext_dtype, dtypes) = self.storage_dtypes(args)?;
        let storage_results = self
            .0
            .storage()
            .finalize(dtypes.args(args.options), states)?;
        self.0.lift_array(ext_dtype, storage_results)
    }

    fn finalize_scalar(
        &self,
        args: AggregateArgs<'_, Self::Options>,
        partial: &Self::Partial,
    ) -> VortexResult<Scalar> {
        self.0
            .lift_scalar(ext_dtype(args.dtype)?, partial.final_scalar()?)
    }
}
