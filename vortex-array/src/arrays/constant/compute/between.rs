// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;

use crate::ArrayRef;
use crate::IntoArray;
use crate::array::ArrayView;
use crate::arrays::Constant;
use crate::arrays::ConstantArray;
use crate::dtype::DType;
use crate::scalar::Scalar;
use crate::scalar_fn::fns::between::BetweenOptions;
use crate::scalar_fn::fns::between::BetweenReduce;

impl BetweenReduce for Constant {
    fn between(
        array: ArrayView<'_, Constant>,
        lower: &ArrayRef,
        upper: &ArrayRef,
        options: &BetweenOptions,
    ) -> VortexResult<Option<ArrayRef>> {
        // Can reduce if everything is constant
        if let Some(((constant, lower), upper)) = Some(array.scalar().clone())
            .zip(lower.as_constant())
            .zip(upper.as_constant())
        {
            let BetweenOptions {
                lower_strict,
                upper_strict,
            } = options;

            // A null operand makes its comparison null, and the two comparisons combine with
            // Kleene AND, so a false comparison still decides the result.
            let compare = |lhs: &Scalar, rhs: &Scalar, strict: bool| {
                (!lhs.is_null() && !rhs.is_null())
                    .then(|| if strict { lhs < rhs } else { lhs <= rhs })
            };
            let lower_result = compare(&lower, &constant, lower_strict.is_strict());
            let upper_result = compare(&constant, &upper, upper_strict.is_strict());
            let result = match (lower_result, upper_result) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            };

            let nullability = array.dtype().nullability()
                | lower.dtype().nullability()
                | upper.dtype().nullability();
            let scalar = match result {
                Some(result) => Scalar::bool(result, nullability),
                None => Scalar::null(DType::Bool(nullability)),
            };
            return Ok(Some(ConstantArray::new(scalar, array.len()).into_array()));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_error::VortexResult;
    use vortex_mask::Mask;

    use crate::IntoArray;
    use crate::VortexSessionExecute;
    use crate::array_session;
    use crate::arrays::BoolArray;
    use crate::arrays::Constant;
    use crate::arrays::ConstantArray;
    use crate::assert_arrays_eq;
    use crate::dtype::DType;
    use crate::dtype::Nullability;
    use crate::dtype::PType;
    use crate::optimizer::ArrayOptimizer;
    use crate::scalar::Scalar;
    use crate::scalar_fn::fns::between::Between;
    use crate::scalar_fn::fns::between::BetweenOptions;
    use crate::scalar_fn::fns::between::BetweenReduce;
    use crate::scalar_fn::fns::between::StrictComparison;

    const NON_STRICT: BetweenOptions = BetweenOptions {
        lower_strict: StrictComparison::NonStrict,
        upper_strict: StrictComparison::NonStrict,
    };

    fn null_i32() -> Scalar {
        Scalar::null(DType::Primitive(PType::I32, Nullability::Nullable))
    }

    /// A null operand makes its comparison null rather than ordering first, so the folded result
    /// matches executing the comparisons: null, unless the other comparison is false.
    #[rstest]
    #[case::null_value(
        Scalar::null(DType::Bool(Nullability::Nullable)),
        Scalar::from(false),
        Scalar::from(true),
        None
    )]
    #[case::null_lower_false_upper(Scalar::from(5i32), null_i32(), Scalar::from(3i32), Some(false))]
    #[case::null_lower_true_upper(Scalar::from(2i32), null_i32(), Scalar::from(3i32), None)]
    fn test_between_constant_with_null_operand(
        #[case] value: Scalar,
        #[case] lower: Scalar,
        #[case] upper: Scalar,
        #[case] expected: Option<bool>,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let array = Between::try_new(
            ConstantArray::new(value, 3).into_array(),
            ConstantArray::new(lower, 3).into_array(),
            ConstantArray::new(upper, 3).into_array(),
            NON_STRICT,
        )?
        .into_array();

        assert_arrays_eq!(array, BoolArray::from_iter([expected; 3]), &mut ctx);
        assert_arrays_eq!(
            array.optimize()?,
            BoolArray::from_iter([expected; 3]),
            &mut ctx
        );
        // Filtering pushes down to all-constant children, which the reduce rule folds.
        assert_arrays_eq!(
            array.filter(Mask::from_iter([true, false, true]))?,
            BoolArray::from_iter([expected; 2]),
            &mut ctx
        );
        Ok(())
    }

    /// `Between` answers null bounds before it reaches the constant fold, so this checks the
    /// fold's Kleene semantics directly.
    #[rstest]
    #[case::null_value(
        Scalar::null(DType::Bool(Nullability::Nullable)),
        Scalar::from(false),
        Scalar::from(true),
        None
    )]
    #[case::null_lower_false_upper(Scalar::from(5i32), null_i32(), Scalar::from(3i32), Some(false))]
    #[case::null_lower_true_upper(Scalar::from(2i32), null_i32(), Scalar::from(3i32), None)]
    #[case::false_lower_null_upper(Scalar::from(2i32), Scalar::from(3i32), null_i32(), Some(false))]
    fn test_between_reduce_constant_with_null_operand(
        #[case] value: Scalar,
        #[case] lower: Scalar,
        #[case] upper: Scalar,
        #[case] expected: Option<bool>,
    ) -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let array = ConstantArray::new(value, 3);
        let result = <Constant as BetweenReduce>::between(
            array.as_view(),
            &ConstantArray::new(lower, 3).into_array(),
            &ConstantArray::new(upper, 3).into_array(),
            &NON_STRICT,
        )?
        .expect("all-constant operands fold");
        assert_arrays_eq!(result, BoolArray::from_iter([expected; 3]), &mut ctx);
        Ok(())
    }
}
