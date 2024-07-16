use std::ops::BitAnd;

use arrow_buffer::BooleanBuffer;
use vortex_dtype::{match_each_native_ptype, NativePType};
use vortex_error::VortexResult;
use vortex_expr::Operator;

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::compute::CompareFn;
use crate::{Array, IntoArray, IntoArrayVariant};

impl CompareFn for PrimitiveArray {
    fn compare(&self, other: &Array, predicate: Operator) -> VortexResult<Array> {
        let flattened = other.clone().into_primitive()?;

        let matching_idxs = match_each_native_ptype!(self.ptype(), |$T| {
            let predicate_fn = &predicate.to_predicate::<$T>();
            apply_predicate(self.maybe_null_slice::<$T>(), flattened.maybe_null_slice::<$T>(), predicate_fn)
        });

        let present = self
            .validity()
            .to_logical(self.len())
            .to_present_null_buffer()?
            .into_inner();
        let present_other = flattened
            .validity()
            .to_logical(self.len())
            .to_present_null_buffer()?
            .into_inner();

        Ok(BoolArray::from(matching_idxs.bitand(&present).bitand(&present_other)).into_array())
    }
}

fn apply_predicate<T: NativePType, F: Fn(&T, &T) -> bool>(
    lhs: &[T],
    rhs: &[T],
    f: F,
) -> BooleanBuffer {
    let matches = lhs.iter().zip(rhs.iter()).map(|(lhs, rhs)| f(lhs, rhs));
    BooleanBuffer::from_iter(matches)
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use crate::compute::compare;
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
        let arr = PrimitiveArray::from_nullable_vec(vec![
            Some(1i32),
            Some(2),
            Some(3),
            Some(4),
            None,
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            None,
            Some(9),
            None,
        ])
        .into_array();

        let matches = compare(&arr, &arr, Operator::Eq)?.into_bool()?;
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7, 8, 10]);

        let matches = compare(&arr, &arr, Operator::NotEq)?.into_bool()?;
        let empty: [u64; 0] = [];
        assert_eq!(to_int_indices(matches), empty);

        let other = PrimitiveArray::from_nullable_vec(vec![
            Some(1i32),
            Some(2),
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            None,
            Some(10),
            None,
        ])
        .into_array();

        let matches = compare(&arr, &other, Operator::Lte)?.into_bool()?;
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7, 8, 10]);

        let matches = compare(&arr, &other, Operator::Lt)?.into_bool()?;
        assert_eq!(to_int_indices(matches), [5u64, 6, 7, 8, 10]);

        let matches = compare(&other, &arr, Operator::Gte)?.into_bool()?;
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7, 8, 10]);

        let matches = compare(&other, &arr, Operator::Gt)?.into_bool()?;
        assert_eq!(to_int_indices(matches), [5u64, 6, 7, 8, 10]);
        Ok(())
    }
}
