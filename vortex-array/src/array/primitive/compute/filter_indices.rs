use std::ops::{BitAnd, BitOr};

use arrow_buffer::BooleanBuffer;
use vortex_dtype::{match_each_native_ptype, NativePType};
use vortex_error::{vortex_bail, VortexResult};
use vortex_expr::{Disjunction, Predicate, Value};

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::compute::filter_indices::FilterIndicesFn;
use crate::{Array, ArrayTrait, IntoArray};

impl FilterIndicesFn for PrimitiveArray {
    fn filter_indices(&self, disjunction: &Disjunction) -> VortexResult<Array> {
        let conjunction_indices = disjunction.iter().map(|conj| {
            conj.iter()
                .map(|pred| indices_matching_predicate(self, pred))
                .reduce(|a, b| Ok(a?.bitand(&b?)))
                .unwrap()
        });
        let present_buf = self
            .validity()
            .to_logical(self.len())
            .to_present_null_buffer()?
            .into_inner();

        let bitset: VortexResult<BooleanBuffer> = conjunction_indices
            .reduce(|a, b| Ok(a?.bitor(&b?)))
            .map(|bitset| Ok(bitset?.bitand(&present_buf)))
            .unwrap_or_else(|| Ok(BooleanBuffer::new_set(self.len())));

        Ok(BoolArray::from(bitset?).into_array())
    }
}

fn indices_matching_predicate(
    arr: &PrimitiveArray,
    predicate: &Predicate,
) -> VortexResult<BooleanBuffer> {
    if !predicate.lhs.path().is_empty() {
        vortex_bail!("Invalid path for primitive array")
    }

    let rhs = match &predicate.rhs {
        Value::Field(_) => {
            vortex_bail!("Cannot apply field reference to primitive array")
        }
        Value::Literal(scalar) => scalar,
    };

    let matching_idxs = match_each_native_ptype!(arr.ptype(), |$T| {
        let rhs_typed: $T = rhs.try_into().unwrap();
        let predicate_fn = &predicate.op.to_predicate::<$T>();
        apply_predicate(arr.maybe_null_slice::<$T>(), &rhs_typed, predicate_fn)
    });

    Ok(matching_idxs)
}

fn apply_predicate<T: NativePType, F: Fn(&T, &T) -> bool>(
    lhs: &[T],
    rhs: &T,
    f: F,
) -> BooleanBuffer {
    let matches = lhs.iter().map(|lhs| f(lhs, rhs));
    BooleanBuffer::from_iter(matches)
}

#[cfg(test)]
mod test {
    use vortex_dtype::field::FieldPath;
    use vortex_expr::{lit, Conjunction, FieldPathOperations};

    use super::*;
    use crate::validity::Validity;
    use crate::IntoCanonical;

    fn apply_conjunctive_filter(arr: &PrimitiveArray, conj: Conjunction) -> VortexResult<Array> {
        arr.filter_indices(&Disjunction::from_iter([conj]))
    }

    fn to_int_indices(filtered_primitive: BoolArray) -> Vec<u64> {
        filtered_primitive
            .boolean_buffer()
            .set_indices()
            .map(|i| i as u64)
            .collect()
    }

    #[test]
    fn test_basic_filter() {
        let arr = PrimitiveArray::from_nullable_vec(vec![
            Some(1u32),
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

        let field = FieldPath::root();
        let filtered_primitive =
            apply_conjunctive_filter(&arr, Conjunction::from(field.lt(lit(5u32))))
                .unwrap()
                .into_canonical()
                .unwrap()
                .into_bool()
                .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [0u64, 1, 2, 3]);

        let filtered_primitive =
            apply_conjunctive_filter(&arr, Conjunction::from(field.gt(lit(5u32))))
                .unwrap()
                .into_canonical()
                .unwrap()
                .into_bool()
                .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [6u64, 7, 8, 10]);

        let filtered_primitive =
            apply_conjunctive_filter(&arr, Conjunction::from(field.equal(lit(5u32))))
                .unwrap()
                .into_canonical()
                .unwrap()
                .into_bool()
                .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [5u64]);

        let filtered_primitive =
            apply_conjunctive_filter(&arr, Conjunction::from(field.gte(lit(5u32))))
                .unwrap()
                .into_canonical()
                .unwrap()
                .into_bool()
                .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [5u64, 6, 7, 8, 10]);

        let filtered_primitive =
            apply_conjunctive_filter(&arr, Conjunction::from(field.lte(lit(5u32))))
                .unwrap()
                .into_canonical()
                .unwrap()
                .into_bool()
                .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [0u64, 1, 2, 3, 5]);
    }

    #[test]
    fn test_multiple_predicates() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPath::root();
        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction::from_iter([field.lt(lit(5u32)), field.gt(lit(2u32))]),
        )
        .unwrap()
        .into_canonical()
        .unwrap()
        .into_bool()
        .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [2u64, 3])
    }

    #[test]
    fn test_disjoint_predicates() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPath::root();
        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction::from_iter([field.lt(lit(5u32)), field.gt(lit(5u32))]),
        )
        .unwrap()
        .into_canonical()
        .unwrap()
        .into_bool()
        .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        let expected: [u64; 0] = [];
        assert_eq!(filtered, expected)
    }

    #[test]
    fn test_disjunctive_predicate() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPath::root();
        let c1 = Conjunction::from(field.lt(lit(5u32)));
        let c2 = Conjunction::from(field.gt(lit(5u32)));

        let disj = Disjunction::from_iter([c1, c2]);
        let filtered_primitive = arr
            .filter_indices(&disj)
            .unwrap()
            .into_canonical()
            .unwrap()
            .into_bool()
            .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [0u64, 1, 2, 3, 5, 6, 7, 8, 9])
    }

    #[test]
    fn test_invalid_path_err() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPath::from_name("some_field");
        apply_conjunctive_filter(
            &arr,
            Conjunction::from_iter([field.lt(lit(5u32)), field.gt(lit(5u32))]),
        )
        .expect_err("Cannot apply field reference to primitive array");
    }
}
