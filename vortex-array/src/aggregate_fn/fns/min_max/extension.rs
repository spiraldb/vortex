// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;
use vortex_session::registry::CachedId;

use super::MinMax;
use super::MinMaxPartial;
use super::MinMaxResult;
use super::NAMES;
use super::make_minmax_dtype;
use super::min_max;
use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::VortexSessionExecute;
use crate::aggregate_fn::AggregateArgs;
use crate::aggregate_fn::AggregateFnId;
use crate::aggregate_fn::ExtensionDelegate;
use crate::aggregate_fn::NumericalAggregateOpts;
use crate::aggregate_fn::StorageLifter;
use crate::arrays::ExtensionArray;
use crate::arrays::Struct;
use crate::arrays::StructArray;
use crate::arrays::struct_::StructArrayExt;
use crate::dtype::DType;
use crate::dtype::Nullability;
use crate::dtype::extension::ExtDTypeRef;
use crate::legacy_session;
use crate::scalar::Scalar;

/// Lifts [`MinMax`] over an extension's storage into extension-typed extrema.
#[derive(Clone, Debug)]
pub(super) struct MinMaxLifter;

impl StorageLifter for MinMaxLifter {
    type Storage = MinMax;

    fn id(&self) -> AggregateFnId {
        static ID: CachedId = CachedId::new("vortex.min_max.extension");
        *ID
    }

    fn storage(&self) -> Self::Storage {
        MinMax
    }

    fn aggregate_storage(
        &self,
        options: &NumericalAggregateOpts,
        storage: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let dtype = make_minmax_dtype(storage.dtype());
        Ok(match min_max(storage, ctx, *options)? {
            Some(MinMaxResult { min, max }) => Scalar::struct_(dtype, vec![min, max]),
            None => Scalar::null(dtype),
        })
    }

    fn lift_return_dtype(&self, ext_dtype: &ExtDTypeRef, _storage_dtype: DType) -> Option<DType> {
        Some(make_minmax_dtype(&DType::Extension(ext_dtype.clone())))
    }

    fn lift_scalar(&self, ext_dtype: &ExtDTypeRef, storage_result: Scalar) -> VortexResult<Scalar> {
        let return_dtype = make_minmax_dtype(&DType::Extension(ext_dtype.clone()));
        let Some(MinMaxResult { min, max }) = MinMaxResult::from_scalar(&storage_result)? else {
            return Ok(Scalar::null(return_dtype));
        };
        let non_nullable_ext_dtype = ext_dtype.with_nullability(Nullability::NonNullable);
        Ok(Scalar::struct_(
            return_dtype,
            vec![
                Scalar::extension_ref(non_nullable_ext_dtype.clone(), min),
                Scalar::extension_ref(non_nullable_ext_dtype, max),
            ],
        ))
    }

    fn lift_array(
        &self,
        ext_dtype: &ExtDTypeRef,
        storage_results: ArrayRef,
    ) -> VortexResult<ArrayRef> {
        let storage_results = match storage_results.as_opt::<Struct>() {
            Some(results) => results.into_owned(),
            None => storage_results
                .execute::<StructArray>(&mut legacy_session().create_execution_ctx())?,
        };
        let non_nullable_ext_dtype = ext_dtype.with_nullability(Nullability::NonNullable);
        let fields = storage_results.iter_unmasked_fields().map(|field| {
            ExtensionArray::new(non_nullable_ext_dtype.clone(), field.clone()).into_array()
        });
        Ok(StructArray::try_new(
            NAMES.clone(),
            fields,
            storage_results.len(),
            storage_results.struct_validity(),
        )?
        .into_array())
    }
}

pub(super) fn accumulate_extension(
    args: AggregateArgs<'_, NumericalAggregateOpts>,
    partial: &mut MinMaxPartial,
    array: &ExtensionArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<()> {
    let result = ExtensionDelegate::new(MinMaxLifter).aggregate(
        &NumericalAggregateOpts::default(),
        array,
        ctx,
    )?;
    partial.merge(args, MinMaxResult::from_scalar(&result)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use vortex_error::VortexResult;

    use super::MinMaxLifter;
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::aggregate_fn::Accumulator;
    use crate::aggregate_fn::DynAccumulator;
    use crate::aggregate_fn::ExtensionDelegate;
    use crate::aggregate_fn::NumericalAggregateOpts;
    use crate::aggregate_fn::fns::min_max::MinMaxResult;
    use crate::arrays::ExtensionArray;
    use crate::arrays::PrimitiveArray;
    use crate::dtype::DType;
    use crate::dtype::Nullability;
    use crate::extension::datetime::TimeUnit;
    use crate::extension::datetime::Timestamp;
    use crate::scalar::Scalar;

    #[test]
    fn delegate_lifts_storage_extrema() -> VortexResult<()> {
        let ext_dtype = Timestamp::new(TimeUnit::Milliseconds, Nullability::Nullable).erased();
        let array = ExtensionArray::new(
            ext_dtype.clone(),
            PrimitiveArray::from_option_iter([Some(3_000i64), None, Some(1_000)]).into_array(),
        )
        .into_array();

        let mut ctx = crate::legacy_session().create_execution_ctx();
        let mut acc = Accumulator::try_new(
            ExtensionDelegate::new(MinMaxLifter),
            NumericalAggregateOpts::default(),
            DType::Extension(ext_dtype.clone()),
        )?;
        acc.accumulate(&array, &mut ctx)?;

        let non_nullable = ext_dtype.with_nullability(Nullability::NonNullable);
        let extremum = |value: i64| {
            Scalar::extension_ref(
                non_nullable.clone(),
                Scalar::primitive(value, Nullability::NonNullable),
            )
        };
        assert_eq!(
            MinMaxResult::from_scalar(&acc.finish()?)?,
            Some(MinMaxResult {
                min: extremum(1_000),
                max: extremum(3_000),
            })
        );
        Ok(())
    }
}
