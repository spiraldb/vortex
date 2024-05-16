use itertools::{Itertools};
use vortex_dtype::{match_each_native_ptype, NativePType};
use vortex_error::{vortex_bail, VortexResult};
use vortex_expr::expressions::{Conjunction, Disjunction, Predicate, Value};
use vortex_expr::operators::Operator;

use crate::{Array, ArrayTrait, IntoArray};
use crate::array::primitive::PrimitiveArray;
use crate::compute::filter_indices::FilterIndicesFn;
use crate::validity::Validity;

impl FilterIndicesFn for PrimitiveArray {
    fn apply_disjunctive_filter(&self, predicate: &Disjunction) -> VortexResult<Array> {
        let map = predicate.conjunctions.iter()
            .map(|conj| {
                BoolMultiZip(
                    conj.predicates.iter().map(|pred| {
                        self.indices_matching_predicate(pred).unwrap()
                    })
                        .map(|a| a.into_iter()).collect_vec(),
                    Comparison::All).collect_vec().into_iter()
            }).collect_vec();
        let bitmask = BoolMultiZip(map, Comparison::Any).collect_vec();
        let indices = bitmask.iter()
            .enumerate()
            .filter(|(_, &v)| v)
            .map(|(idx, _)| (idx + 1) as u64)
            .collect_vec();
        Ok(PrimitiveArray::from_vec(indices, Validity::AllValid).into_array())
    }

    fn apply_conjunctive_filter(&self, conj: &Conjunction) -> VortexResult<Array> {
        let bitmask = BoolMultiZip(
            conj.predicates.iter().map(|pred| {
                self.indices_matching_predicate(pred).unwrap()
            })
                .map(|a| a.into_iter()).collect_vec(),
            Comparison::All)
            .collect_vec();
        let indices = bitmask.iter()
            .enumerate()
            .filter(|(_, &v)| v)
            .map(|(idx, _)| (idx + 1) as u64)
            .collect_vec();
        Ok(PrimitiveArray::from_vec(indices, Validity::AllValid).into_array())
    }

    fn indices_matching_predicate(&self, predicate: &Predicate) -> VortexResult<Vec<bool>> {
        if predicate.left.first().is_some() {
            vortex_bail!("Invalid path for primitive array")
        }
        let validity = self.validity();
        let rhs = match &predicate.right {
            Value::Field(_) => { vortex_bail!("") }
            Value::Literal(scalar) => { scalar }
        };

        let matching_idxs = match_each_native_ptype!(self.ptype(), |$T| {
        let rhs_typed: $T = rhs.try_into().unwrap();
        let predicate_fn = get_predicate::<$T>(&predicate.op);
        self.typed_data::<$T>().iter().enumerate().filter(|(idx, &v)| {
                predicate_fn(&v, &rhs_typed)
            })
            .filter(|(idx, _)| validity.is_valid(idx.clone()))
            .map(|(idx, _)| idx )
            .collect_vec()
        });
        let mut bitmap = vec![false; self.len()];
        matching_idxs.into_iter().for_each(|idx| bitmap[idx] = true);

        Ok(bitmap)
    }
}

fn get_predicate<T: NativePType>(op: &Operator) -> fn(&T, &T) -> bool {
    match op {
        Operator::EqualTo => {
            PartialEq::eq
        }
        Operator::NotEqualTo => {
            PartialEq::ne
        }
        Operator::GreaterThan => {
            PartialOrd::gt
        }
        Operator::GreaterThanOrEqualTo => {
            PartialOrd::ge
        }
        Operator::LessThan => {
            PartialOrd::lt
        }
        Operator::LessThanOrEqualTo => {
            PartialOrd::le
        }
    }
}

enum Comparison {
    Any,
    All,
}

/// Zip together an arbitrary number of boolean iterators
struct BoolMultiZip(Vec<std::vec::IntoIter<bool>>, Comparison);

impl Iterator for BoolMultiZip {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        let zipped = self.0
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Option<Vec<_>>>();

        match self.1 {
            Comparison::Any => zipped.map(|inner| inner.iter().any(|&v| v)),
            Comparison::All => zipped.map(|inner| inner.iter().all(|&v| v))
        }
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::field_paths::{FieldPathBuilder};
    use vortex_expr::expressions::lit;
    use vortex_expr::field_paths::FieldPathOperations;
    use crate::validity::Validity;
    use super::*;

    #[test]
    fn test_basic_filter() {
        let arr = PrimitiveArray::from_vec(
            vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9], Validity::AllValid);

        let field = FieldPathBuilder::new().build();
        let filtered_primitive = arr.apply_conjunctive_filter(&Conjunction {
            predicates: vec![field.clone().lt(lit(5u32))]
        }).unwrap()
            .flatten_primitive().unwrap();
        let filtered = filtered_primitive
            .typed_data::<u64>();
        assert_eq!(filtered, [1u64, 2, 3, 4]);

        let filtered_primitive = arr.apply_conjunctive_filter(&Conjunction {
            predicates: vec![field.clone().gt(lit(5u32))]
        }).unwrap()
            .flatten_primitive().unwrap();
        let filtered = filtered_primitive
            .typed_data::<u64>();
        assert_eq!(filtered, [6u64, 7, 8, 9]);

        let filtered_primitive = arr.apply_conjunctive_filter(&Conjunction {
            predicates: vec![field.clone().eq(lit(5u32))]
        }).unwrap()
            .flatten_primitive().unwrap();
        let filtered = filtered_primitive
            .typed_data::<u64>();
        assert_eq!(filtered, [5]);

        let filtered_primitive = arr.apply_conjunctive_filter(&Conjunction {
            predicates: vec![field.clone().gte(lit(5u32))]
        }).unwrap()
            .flatten_primitive().unwrap();
        let filtered = filtered_primitive
            .typed_data::<u64>();
        assert_eq!(filtered, [5u64, 6, 7, 8, 9]);

        let filtered_primitive = arr.apply_conjunctive_filter(&Conjunction {
            predicates: vec![field.clone().lte(lit(5u32))]
        }).unwrap()
            .flatten_primitive().unwrap();
        let filtered = filtered_primitive
            .typed_data::<u64>();
        assert_eq!(filtered, [1u64, 2, 3, 4, 5]);
    }

    #[test]
    fn test_multiple_predicates() {
        let arr = PrimitiveArray::from_vec(
            vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPathBuilder::new().build();
        let filtered_primitive = arr.apply_conjunctive_filter(&Conjunction {
            predicates: vec![
                field.clone().lt(lit(5u32)),
                field.clone().gt(lit(2u32)),
            ]
        }).unwrap()
            .flatten_primitive().unwrap();
        let filtered = filtered_primitive
            .typed_data::<u64>();
        assert_eq!(filtered, [3, 4])
    }

    #[test]
    fn test_disjoint_predicates() {
        let arr = PrimitiveArray::from_vec(
            vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPathBuilder::new().build();
        let filtered_primitive = arr.apply_conjunctive_filter(&Conjunction {
            predicates: vec![
                field.clone().lt(lit(5u32)),
                field.clone().gt(lit(5u32)),
            ]
        }).unwrap()
            .flatten_primitive().unwrap();
        let filtered = filtered_primitive
            .typed_data::<u64>();
        assert_eq!(filtered, [])
    }

    #[test]
    fn test_disjunctive_predicate() {
        let arr = PrimitiveArray::from_vec(
            vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPathBuilder::new().build();
        let c1 = Conjunction {
            predicates: vec![
                field.clone().lt(lit(5u32)),
            ]
        };
        let c2 = Conjunction {
            predicates: vec![
                field.clone().gt(lit(5u32)),
            ]
        };

        let disj = Disjunction { conjunctions: vec![c1, c2] };
        let filtered_primitive = arr.apply_disjunctive_filter(&disj).unwrap()
            .flatten_primitive().unwrap();
        let filtered = filtered_primitive
            .typed_data::<u64>();
        assert_eq!(filtered, [1u64, 2, 3, 4, 6, 7, 8, 9, 10])
    }
}
