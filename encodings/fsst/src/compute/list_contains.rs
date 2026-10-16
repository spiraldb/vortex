// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::scalar_fn::fns::binary::CompareKernel;
use vortex_array::scalar_fn::fns::list_contains::ListContainsElementKernel;
use vortex_array::scalar_fn::fns::list_contains::ListContainsOptions;
use vortex_array::scalar_fn::fns::list_contains::PreparedSet;
use vortex_array::scalar_fn::fns::operators::CompareOperator;
use vortex_error::VortexResult;
use vortex_mask::Mask;

use crate::FSST;

// A few comparisons can search the compressed codes directly, avoiding string materialization.
const MAX_COMPRESSED_SET_SIZE: usize = 8;

impl ListContainsElementKernel for FSST {
    fn list_contains(
        list: &ArrayRef,
        element: ArrayView<'_, Self>,
        options: &ListContainsOptions,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        let Some(set) = PreparedSet::set_of(list) else {
            return Ok(None);
        };
        let list = set.list().as_list();
        if list.len() > MAX_COMPRESSED_SET_SIZE {
            return Ok(None);
        }
        let Some(members) = list.elements() else {
            return Ok(None);
        };

        let mut matches = Mask::AllFalse(element.len());
        for scalar in members.into_iter().filter(|member| !member.is_null()) {
            let constant = ConstantArray::new(scalar, element.len()).into_array();
            let Some(equal) =
                <Self as CompareKernel>::compare(element, &constant, CompareOperator::Eq, ctx)?
            else {
                return Ok(None);
            };
            let equal = equal.execute::<BoolArray>(ctx)?;
            matches = matches | &Mask::from(equal.to_bit_buffer());
        }
        Ok(Some(set.result_from_bits(
            matches.to_bit_buffer(),
            element.validity()?,
            element.dtype(),
            options,
        )?))
    }
}
#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_array::VortexSessionExecute;
    use vortex_array::array_session;
    use vortex_array::arrays::VarBinArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability;
    use vortex_array::scalar::Scalar;
    use vortex_array::scalar_fn::fns::list_contains::ListContains;
    use vortex_array::scalar_fn::fns::list_contains::PreparedSetData;
    use vortex_buffer::ByteBuffer;

    use super::*;
    use crate::fsst_compress;
    use crate::fsst_train_compressor;

    #[rstest]
    #[case::empty(vec![])]
    #[case::singleton(vec![Some("Bulgaria")])]
    #[case::empty_string(vec![Some("")])]
    #[case::several(vec![Some("Bulgaria"), Some("日本語"), Some("missing")])]
    #[case::null_only(vec![None])]
    #[case::with_null(vec![None, Some("Bulgaria")])]
    #[case::duplicates(vec![Some("Bulgaria"), Some("Bulgaria"), Some("")])]
    #[case::four(vec![Some("Bulgaria"), None, Some("日本語"), Some("")])]
    #[case::eight(vec![Some("Bulgaria"), None, Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f")])]
    #[case::larger_fallback(vec![Some("Bulgaria"), Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), Some("f"), Some("g"), Some("h")])]
    fn prepared_membership_uses_compressed_comparisons(
        #[case] members: Vec<Option<&str>>,
        #[values(false, true)] sql_null_semantics: bool,
        #[values(false, true)] binary: bool,
        #[values(false, true)] nullable: bool,
    ) -> VortexResult<()> {
        let session = array_session();
        crate::initialize(&session);
        let mut ctx = session.create_execution_ctx();
        let nullability = Nullability::from(nullable);
        let dtype = if binary {
            DType::Binary(nullability)
        } else {
            DType::Utf8(nullability)
        };
        let element_dtype = dtype.as_nullable();
        let values = [
            Some(""),
            if nullable { None } else { Some("missing-row") },
            Some("Bulgaria"),
            Some("日本語"),
            Some("a long string that uses external storage"),
            Some("other"),
        ];
        let input = VarBinArray::from_iter(values, dtype).into_array();
        let member = |value: Option<&str>| match value {
            Some(value) if binary => Scalar::binary(
                ByteBuffer::copy_from(value.as_bytes()),
                Nullability::Nullable,
            ),
            Some(value) => Scalar::utf8(value, Nullability::Nullable),
            None => Scalar::null(element_dtype.clone()),
        };
        let list = Scalar::list(
            element_dtype.clone(),
            members.iter().copied().map(member).collect::<Vec<_>>(),
            Nullability::NonNullable,
        );
        let set = PreparedSetData::try_new(list.clone())?;
        let options = ListContainsOptions { sql_null_semantics };
        let expected = set.contains(&input, &options, &mut ctx)?;
        let list = PreparedSet::try_new_array(list, input.len())?;
        let compressor = fsst_train_compressor(&input, &mut ctx)?;
        let compressed = fsst_compress(&input, &compressor, &mut ctx)?;
        let result = <FSST as ListContainsElementKernel>::list_contains(
            &list,
            compressed.as_view(),
            &options,
            &mut ctx,
        )?;
        assert_eq!(result.is_some(), members.len() <= MAX_COMPRESSED_SET_SIZE);
        if let Some(result) = result {
            assert_arrays_eq!(result, &expected, &mut ctx);
        }

        let compressed = compressed.into_array();
        let result = ListContains::try_new_opts(list.clone(), compressed.clone(), options)?
            .into_array()
            .execute::<BoolArray>(&mut ctx)?;
        assert_arrays_eq!(result, &expected, &mut ctx);
        let sliced = ListContains::try_new_opts(
            PreparedSet::try_new_array(set.list().clone(), 4)?,
            compressed.slice(1..5)?,
            options,
        )?
        .into_array()
        .execute::<BoolArray>(&mut ctx)?;
        assert_arrays_eq!(sliced, expected.slice(1..5)?, &mut ctx);
        Ok(())
    }
}
