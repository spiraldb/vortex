use std::ops::BitAnd;

use arrow_buffer::BooleanBuffer;
use vortex_dtype::{match_each_native_ptype, NativePType};
use vortex_error::VortexResult;
use vortex_expr::operators::Operator;

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::compute::compare::CompareArraysFn;
use crate::{Array, ArrayTrait, IntoArray};

impl CompareArraysFn for PrimitiveArray {
    fn compare_arrays(&self, other: &Array, predicate: Operator) -> VortexResult<Array> {
        let flattened = other.clone().flatten_primitive()?;

        let matching_idxs = match_each_native_ptype!(self.ptype(), |$T| {
            let predicate_fn = &predicate.to_predicate::<$T>();
            apply_predicate(self.typed_data::<$T>(), flattened.typed_data::<$T>(), predicate_fn)
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
    use crate::ToArray;

    fn to_int_indices(filtered_primitive: BoolArray) -> Vec<u64> {
        let filtered = filtered_primitive
            .boolean_buffer()
            .iter()
            .enumerate()
            .flat_map(|(idx, v)| if v { Some(idx as u64) } else { None })
            .collect_vec();
        filtered
    }

    #[test]
    fn test_basic_filter() {
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
        ]);

        let matches = arr
            .compare_arrays(&arr.to_array(), Operator::EqualTo)
            .unwrap()
            .flatten_bool()
            .unwrap();
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7, 8, 10]);

        let matches = arr
            .compare_arrays(&arr.to_array(), Operator::NotEqualTo)
            .unwrap()
            .flatten_bool()
            .unwrap();
        assert_eq!(to_int_indices(matches), []);

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
        ]);

        let matches = arr
            .compare_arrays(&other.to_array(), Operator::LessThanOrEqualTo)
            .unwrap()
            .flatten_bool()
            .unwrap();
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7, 8, 10]);

        let matches = arr
            .compare_arrays(&other.to_array(), Operator::LessThan)
            .unwrap()
            .flatten_bool()
            .unwrap();
        assert_eq!(to_int_indices(matches), [5u64, 6, 7, 8, 10]);

        let matches = other
            .compare_arrays(&arr.to_array(), Operator::GreaterThanOrEqualTo)
            .unwrap()
            .flatten_bool()
            .unwrap();
        assert_eq!(to_int_indices(matches), [0u64, 1, 2, 3, 5, 6, 7, 8, 10]);

        let matches = other
            .compare_arrays(&arr.to_array(), Operator::GreaterThan)
            .unwrap()
            .flatten_bool()
            .unwrap();
        assert_eq!(to_int_indices(matches), [5u64, 6, 7, 8, 10]);
    }
}
