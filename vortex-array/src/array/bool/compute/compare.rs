use std::ops::{BitAnd, BitOr, BitXor, Not};

use vortex_error::VortexResult;
use vortex_expr::Operator;

use crate::array::bool::BoolArray;
use crate::compute::CompareFn;
use crate::{Array, IntoArray, IntoArrayVariant};

impl CompareFn for BoolArray {
    // TODO(aduffy): replace these with Arrow compute kernels.
    fn compare(&self, other: &Array, op: Operator) -> VortexResult<Array> {
        let flattened = other.clone().into_bool()?;
        let lhs = self.boolean_buffer();
        let rhs = flattened.boolean_buffer();
        let result_buf = match op {
            Operator::Eq => lhs.bitxor(&rhs).not(),
            Operator::NotEq => lhs.bitxor(&rhs),

            Operator::Gt => lhs.bitand(&rhs.not()),
            Operator::Gte => lhs.bitor(&rhs.not()),
            Operator::Lt => lhs.not().bitand(&rhs),
            Operator::Lte => lhs.not().bitor(&rhs),
        };
        Ok(BoolArray::from(
            self.validity()
                .to_logical(self.len())
                .to_null_buffer()?
                .map(|nulls| result_buf.bitand(&nulls.into_inner()))
                .unwrap_or(result_buf),
        )
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use crate::compute::compare;
    use crate::validity::Validity;
    use crate::IntoArrayVariant;

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

        let matches = compare(&arr, &arr, Operator::Eq)?.into_bool()?;
        assert_eq!(to_int_indices(matches), [1u64, 2, 3, 4]);

        let matches = compare(&arr, &arr, Operator::NotEq)?.into_bool()?;
        let empty: [u64; 0] = [];
        assert_eq!(to_int_indices(matches), empty);

        let other = BoolArray::from_vec(
            vec![false, false, false, true, true],
            Validity::Array(BoolArray::from(vec![false, true, true, true, true]).into_array()),
        )
        .into_array();

        let matches = compare(&arr, &other, Operator::Lte)?.into_bool()?;
        assert_eq!(to_int_indices(matches), [2u64, 3, 4]);

        let matches = compare(&arr, &other, Operator::Lt)?.into_bool()?;
        assert_eq!(to_int_indices(matches), [4u64]);

        let matches = compare(&other, &arr, Operator::Gte)?.into_bool()?;
        assert_eq!(to_int_indices(matches), [2u64, 3, 4]);

        let matches = compare(&other, &arr, Operator::Gt)?.into_bool()?;
        assert_eq!(to_int_indices(matches), [4u64]);
        Ok(())
    }
}
