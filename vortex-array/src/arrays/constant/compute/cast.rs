// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;

use crate::ArrayRef;
use crate::IntoArray;
use crate::array::ArrayView;
use crate::arrays::Constant;
use crate::arrays::ConstantArray;
use crate::dtype::DType;
use crate::scalar_fn::fns::cast::CastReduce;

impl CastReduce for Constant {
    fn cast(array: ArrayView<'_, Constant>, dtype: &DType) -> VortexResult<Option<ArrayRef>> {
        match array.scalar().cast(dtype) {
            Ok(scalar) => Ok(Some(ConstantArray::new(scalar, array.len()).into_array())),
            Err(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_error::VortexResult;

    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::array_session;
    use crate::arrays::BoolArray;
    use crate::arrays::ConstantArray;
    use crate::arrays::PrimitiveArray;
    use crate::assert_arrays_eq;
    use crate::builtins::ArrayBuiltins;
    use crate::compute::conformance::cast::test_cast_conformance;
    use crate::dtype::DType;
    use crate::dtype::DecimalDType;
    use crate::dtype::Nullability;
    use crate::dtype::PType;
    use crate::scalar::DecimalValue;
    use crate::scalar::Scalar;

    #[rstest]
    #[case(ConstantArray::new(Scalar::from(42u32), 5).into_array())]
    #[case(ConstantArray::new(Scalar::from(-100i32), 10).into_array())]
    #[case(ConstantArray::new(Scalar::from(3.5f32), 3).into_array())]
    #[case(ConstantArray::new(Scalar::from(true), 7).into_array())]
    #[case(ConstantArray::new(Scalar::null_native::<i32>(), 4).into_array())]
    #[case(ConstantArray::new(Scalar::from(255u8), 1).into_array())]
    fn test_cast_constant_conformance(#[case] array: crate::ArrayRef) {
        test_cast_conformance(&array, &mut array_session().create_execution_ctx());
    }

    #[test]
    #[allow(clippy::disallowed_methods)]
    fn test_cast_constant_i64_to_decimal() {
        let target_dtype = DType::Decimal(DecimalDType::new(21, 2), Nullability::NonNullable);
        let casted = ConstantArray::new(Scalar::from(42i64), 5)
            .into_array()
            .cast(target_dtype.clone())
            .unwrap();

        assert_eq!(casted.dtype(), &target_dtype);
        let scalar = casted
            .execute_scalar(0, &mut array_session().create_execution_ctx())
            .unwrap();
        assert_eq!(
            scalar.as_decimal().decimal_value(),
            Some(DecimalValue::I128(4200))
        );
    }

    /// A constant boolean casts to `1` or `0` like the canonical boolean cast kernel does.
    #[rstest]
    #[case(true, 1)]
    #[case(false, 0)]
    fn test_cast_constant_bool_to_primitive(
        #[case] value: bool,
        #[case] expected: i16,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let target_dtype = DType::Primitive(PType::I16, Nullability::Nullable);

        let casted = ConstantArray::new(Scalar::from(value), 3)
            .into_array()
            .cast(target_dtype.clone())?;
        assert_arrays_eq!(
            casted,
            PrimitiveArray::from_option_iter([Some(expected); 3]),
            &mut ctx
        );

        let canonical = BoolArray::from_iter([value; 3])
            .into_array()
            .cast(target_dtype)?;
        assert_arrays_eq!(casted, canonical, &mut ctx);
        Ok(())
    }
}
