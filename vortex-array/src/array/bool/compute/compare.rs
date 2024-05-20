use std::ops::{BitAnd, BitOr, BitXor, Not};

use vortex_error::VortexResult;
use vortex_expr::operators::Operator;

use crate::array::bool::BoolArray;
use crate::compute::compare::CompareArraysFn;
use crate::{Array, ArrayTrait, IntoArray};

impl CompareArraysFn for BoolArray {
    fn compare_arrays(&self, other: &Array, op: Operator) -> VortexResult<Array> {
        let flattened = other.clone().flatten_bool()?;
        let lhs = self.boolean_buffer();
        let rhs = flattened.boolean_buffer();
        let result_buf = match op {
            Operator::EqualTo => lhs.bitxor(&rhs).not(),
            Operator::NotEqualTo => lhs.bitxor(&rhs),

            Operator::GreaterThan => lhs.bitand(&rhs).bitxor(&lhs),
            Operator::GreaterThanOrEqualTo => {
                let gt = lhs.bitand(&rhs).bitxor(&lhs);
                let eq = &lhs.bitxor(&rhs).not();
                gt.bitor(eq)
            }
            Operator::LessThan => lhs.bitor(&rhs).bitxor(&lhs),
            Operator::LessThanOrEqualTo => {
                let eq = lhs.bitxor(&rhs).not();
                let lt = lhs.bitor(&rhs).bitxor(&lhs);
                lt.bitor(&eq)
            }
        };
        let present_buf = self
            .validity()
            .to_logical(self.len())
            .to_present_null_buffer()?
            .into_inner();

        Ok(BoolArray::from(result_buf.bitand(&present_buf)).into_array())
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use crate::compute::compare::compare_arrays;
    use crate::validity::Validity;

    fn to_int_indices(indices_bits: BoolArray) -> Vec<u64> {
        let filtered = indices_bits
            .boolean_buffer()
            .iter()
            .enumerate()
            .flat_map(|(idx, v)| if v { Some(idx as u64) } else { None })
            .collect_vec();
        filtered
    }

    #[test]
    fn test_basic_comparisons() -> VortexResult<()> {
        let arr = BoolArray::from_vec(
            vec![true, true, false, true, false],
            Validity::Array(BoolArray::from(vec![false, true, true, true, true]).into_array()),
        )
        .into_array();

        let matches = compare_arrays(&arr, &arr, Operator::EqualTo)?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [1u64, 2, 3, 4]);

        let matches = compare_arrays(&arr, &arr, Operator::NotEqualTo)?.flatten_bool()?;
        let empty: [u64; 0] = [];
        assert_eq!(to_int_indices(matches), empty);

        let other = BoolArray::from_vec(
            vec![false, false, false, true, true],
            Validity::Array(BoolArray::from(vec![false, true, true, true, true]).into_array()),
        )
        .into_array();

        let matches = compare_arrays(&arr, &other, Operator::LessThanOrEqualTo)?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [2u64, 3, 4]);

        let matches = compare_arrays(&arr, &other, Operator::LessThan)?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [4u64]);

        let matches =
            compare_arrays(&other, &arr, Operator::GreaterThanOrEqualTo)?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [2u64, 3, 4]);

        let matches = compare_arrays(&other, &arr, Operator::GreaterThan)?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [4u64]);
        Ok(())
    }
}
