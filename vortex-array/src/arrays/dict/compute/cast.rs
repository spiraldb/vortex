// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;

use super::Dict;
use super::DictArray;
use crate::ArrayRef;
use crate::IntoArray;
use crate::array::ArrayView;
use crate::arrays::dict::DictArrayExt;
use crate::arrays::dict::DictArraySlotsExt;
use crate::builtins::ArrayBuiltins;
use crate::dtype::DType;
use crate::scalar_fn::fns::cast::CastReduce;

impl CastReduce for Dict {
    fn cast(array: ArrayView<'_, Dict>, dtype: &DType) -> VortexResult<Option<ArrayRef>> {
        // Casting the values also casts the ones no code references, which can fail (out of
        // range, or null when casting to non-nullable) although the dictionary's own values cast
        // fine. Only push the cast down when every value is referenced.
        // TODO(joe): optimize this, could look at accessible values and fill_null not those?
        if !array.has_all_values_referenced() {
            return Ok(None);
        }
        // Null codes must make a cast to non-nullable fail, which a lazily cast codes child does
        // not guarantee (it is never executed when the values are constant).
        if !dtype.is_nullable()
            && (!array.values().validity()?.definitely_no_nulls()
                || !array.codes().validity()?.definitely_no_nulls())
        {
            return Ok(None);
        }
        // Cast the dictionary values to the target type
        let casted_values = array.values().cast(dtype.clone())?;

        // If the codes are nullable but we are casting to non nullable dtype we have to remove nullability from codes as well
        let casted_codes = if array.codes().dtype().is_nullable() && !dtype.is_nullable() {
            array
                .codes()
                .cast(array.codes().dtype().with_nullability(dtype.nullability()))?
        } else {
            array.codes().clone()
        };

        // SAFETY: casting does not alter invariants of the codes
        Ok(Some(
            unsafe {
                DictArray::new_unchecked(casted_codes, casted_values)
                    .set_all_values_referenced(array.has_all_values_referenced())
            }
            .into_array(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use rstest::rstest;
    use vortex_buffer::BitBuffer;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;
    use vortex_session::VortexSession;

    use crate::ArrayRef;
    use crate::Canonical;
    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::arrays::ConstantArray;
    use crate::arrays::Dict;
    use crate::arrays::DictArray;
    use crate::arrays::PrimitiveArray;
    use crate::arrays::dict::DictArraySlotsExt;
    use crate::assert_arrays_eq;
    use crate::builders::dict::dict_encode;
    use crate::builtins::ArrayBuiltins;
    use crate::compute::conformance::cast::test_cast_conformance;
    use crate::dtype::DType;
    use crate::dtype::Nullability;
    use crate::dtype::PType;
    use crate::scalar::Scalar;
    use crate::validity::Validity;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(crate::array_session);

    #[test]
    fn test_cast_dict_to_wider_type() {
        let ctx = &mut SESSION.create_execution_ctx();
        let values = buffer![1i32, 2, 3, 2, 1].into_array();
        let dict = dict_encode(&values, ctx).unwrap();

        let casted = dict
            .into_array()
            .cast(DType::Primitive(PType::I64, Nullability::NonNullable))
            .unwrap();
        assert_eq!(
            casted.dtype(),
            &DType::Primitive(PType::I64, Nullability::NonNullable)
        );

        let decoded = casted.into_array().execute::<PrimitiveArray>(ctx).unwrap();
        assert_arrays_eq!(decoded, PrimitiveArray::from_iter([1i64, 2, 3, 2, 1]), ctx);
    }

    #[test]
    fn test_cast_dict_nullable() {
        let values =
            PrimitiveArray::from_option_iter([Some(10i32), None, Some(20), Some(10), None]);
        let dict = dict_encode(&values.into_array(), &mut SESSION.create_execution_ctx()).unwrap();

        let casted = dict
            .into_array()
            .cast(DType::Primitive(PType::I64, Nullability::Nullable))
            .unwrap();
        assert_eq!(
            casted.dtype(),
            &DType::Primitive(PType::I64, Nullability::Nullable)
        );
    }

    #[test]
    fn test_cast_dict_allvalid_to_nonnullable_and_back() {
        let ctx = &mut SESSION.create_execution_ctx();
        // Create an AllValid dict array (no nulls)
        let values = buffer![10i32, 20, 30, 40].into_array();
        let dict = dict_encode(&values, ctx).unwrap();

        // Verify initial state - codes should be NonNullable, values should be NonNullable
        assert_eq!(dict.codes().dtype().nullability(), Nullability::NonNullable);
        assert_eq!(
            dict.values().dtype().nullability(),
            Nullability::NonNullable
        );

        // Cast to NonNullable (should be identity since already NonNullable)
        let non_nullable = dict
            .clone()
            .into_array()
            .cast(DType::Primitive(PType::I32, Nullability::NonNullable))
            .unwrap();
        assert_eq!(
            non_nullable.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );

        // Check that codes and values are still NonNullable
        let non_nullable_dict = non_nullable.as_::<Dict>();
        assert_eq!(
            non_nullable_dict.codes().dtype().nullability(),
            Nullability::NonNullable
        );
        assert_eq!(
            non_nullable_dict.values().dtype().nullability(),
            Nullability::NonNullable
        );

        // Cast to Nullable
        let nullable = non_nullable
            .cast(DType::Primitive(PType::I32, Nullability::Nullable))
            .unwrap();
        assert_eq!(
            nullable.dtype(),
            &DType::Primitive(PType::I32, Nullability::Nullable)
        );

        // Check that both codes and values are now Nullable
        let nullable_dict = nullable.as_::<Dict>();
        assert_eq!(
            nullable_dict.codes().dtype().nullability(),
            Nullability::NonNullable
        );
        assert_eq!(
            nullable_dict.values().dtype().nullability(),
            Nullability::Nullable
        );

        // Cast back to NonNullable
        let back_to_non_nullable = nullable
            .cast(DType::Primitive(PType::I32, Nullability::NonNullable))
            .unwrap();
        assert_eq!(
            back_to_non_nullable.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );

        // Verify values are unchanged
        let original_values = dict.into_array().execute::<PrimitiveArray>(ctx).unwrap();

        let final_values = back_to_non_nullable.execute::<PrimitiveArray>(ctx).unwrap();
        assert_arrays_eq!(original_values, final_values, ctx);
    }

    #[rstest]
    #[case(dict_encode(&buffer![1i32, 2, 3, 2, 1, 3].into_array(), &mut SESSION.create_execution_ctx()).unwrap().into_array())]
    #[case(dict_encode(&buffer![100u32, 200, 100, 300, 200].into_array(), &mut SESSION.create_execution_ctx()).unwrap().into_array())]
    #[case(dict_encode(&PrimitiveArray::from_option_iter([Some(1i32), None, Some(2), Some(1), None]).into_array(), &mut SESSION.create_execution_ctx()).unwrap().into_array())]
    #[case(dict_encode(&buffer![1.5f32, 2.5, 1.5, 3.5].into_array(), &mut SESSION.create_execution_ctx()).unwrap().into_array())]
    fn test_cast_dict_conformance(#[case] array: ArrayRef) {
        test_cast_conformance(&array, &mut SESSION.create_execution_ctx());
    }

    #[test]
    fn test_cast_dict_with_unreferenced_null_values_to_nonnullable() {
        let ctx = &mut SESSION.create_execution_ctx();
        // Create a dict with nullable values that have unreferenced null entries.
        // Values: [1.0, null, 3.0] (index 1 is null but no code points to it)
        // Codes: [0, 2, 0] (only reference indices 0 and 2, never 1)
        let values = PrimitiveArray::new(
            buffer![1.0f64, 0.0f64, 3.0f64],
            Validity::from(BitBuffer::from(vec![true, false, true])),
        )
        .into_array();
        let codes = buffer![0u32, 2, 0].into_array();
        let dict = DictArray::try_new(codes, values).unwrap();

        // The dict is Nullable (because values are nullable), but all codes point to valid values.
        assert_eq!(
            dict.dtype(),
            &DType::Primitive(PType::F64, Nullability::Nullable)
        );

        // Casting to NonNullable should succeed since all logical values are non-null.
        let result = dict
            .into_array()
            .cast(DType::Primitive(PType::F64, Nullability::NonNullable));
        assert!(
            result.is_ok(),
            "cast to NonNullable should succeed for dict with only unreferenced null values"
        );
        let casted = result.unwrap();
        assert_eq!(
            casted.dtype(),
            &DType::Primitive(PType::F64, Nullability::NonNullable)
        );
        assert_arrays_eq!(casted, PrimitiveArray::from_iter([1.0f64, 3.0, 1.0]), ctx);
    }

    #[test]
    fn test_cast_dict_with_unreferenced_out_of_range_values() -> VortexResult<()> {
        let ctx = &mut SESSION.create_execution_ctx();
        // Values -5, -2, 1, ..., 52, of which only 25 and 37 are referenced. The unreferenced
        // negative values do not fit in u8, but the dictionary's own values do.
        let values = PrimitiveArray::from_iter((0..20).map(|i| -5i32 + 3 * i)).into_array();
        let codes = PrimitiveArray::from_option_iter([None, Some(10u32), Some(14)]).into_array();
        let dict = DictArray::try_new(codes, values)?;

        let casted = dict
            .into_array()
            .cast(DType::Primitive(PType::U8, Nullability::Nullable))?;
        assert_arrays_eq!(
            casted,
            PrimitiveArray::from_option_iter([None, Some(25u8), Some(37)]),
            ctx
        );
        Ok(())
    }

    #[test]
    fn test_cast_dict_with_null_codes_over_constant_values_to_nonnullable() -> VortexResult<()> {
        let ctx = &mut SESSION.create_execution_ctx();
        let values =
            ConstantArray::new(Scalar::binary(buffer![1u8, 2], Nullability::NonNullable), 1)
                .into_array();
        let codes = PrimitiveArray::from_option_iter([None, Some(0u32), Some(0)]).into_array();
        // SAFETY: the only value is referenced by the valid codes.
        let dict = unsafe { DictArray::try_new(codes, values)?.set_all_values_referenced(true) };

        // The null code must make the cast fail rather than be dropped.
        let result = dict
            .into_array()
            .cast(DType::Binary(Nullability::NonNullable))
            .and_then(|casted| casted.execute::<Canonical>(ctx));
        assert!(result.is_err());
        Ok(())
    }
}
