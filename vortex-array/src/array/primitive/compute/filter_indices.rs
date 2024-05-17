use std::ops::{BitAnd, BitOr};

use arrow_buffer::BooleanBuffer;
use itertools::Itertools;
use vortex_dtype::{match_each_native_ptype, NativePType};
use vortex_error::{vortex_bail, VortexResult};
use vortex_expr::expressions::{Disjunction, Predicate, Value};
use vortex_expr::operators::Operator;

use crate::array::primitive::PrimitiveArray;
use crate::compute::filter_indices::FilterIndicesFn;
use crate::validity::Validity;
use crate::{Array, ArrayTrait, IntoArray};

impl FilterIndicesFn for PrimitiveArray {
    fn filter_indices(&self, predicate: &Disjunction) -> VortexResult<Array> {
        let mut conjunction_indices = predicate.conjunctions.iter().flat_map(|conj| {
            BooleanBufferMergeOp::All.merge(
                &mut conj
                    .predicates
                    .iter()
                    .map(|pred| indices_matching_predicate(self, pred).unwrap()),
            )
        });
        let indices = BooleanBufferMergeOp::Any
            .merge(&mut conjunction_indices)
            .map(|bitset| {
                bitset
                    .iter()
                    .enumerate()
                    .flat_map(|(idx, v)| if v { Some(idx as u64) } else { None })
                    .collect_vec()
            })
            .unwrap_or_default();
        Ok(PrimitiveArray::from_vec(indices, Validity::AllValid).into_array())
    }
}

fn indices_matching_predicate(
    arr: &PrimitiveArray,
    predicate: &Predicate,
) -> VortexResult<BooleanBuffer> {
    if predicate.left.head().is_some() {
        vortex_bail!("Invalid path for primitive array")
    }

    let rhs = match &predicate.right {
        Value::Field(_) => {
            vortex_bail!("Right-hand-side fields not yet supported.")
        }
        Value::Literal(scalar) => scalar,
    };

    let present_buf = arr
        .validity()
        .to_logical(arr.len())
        .to_present_null_buffer()?
        .into_inner();

    let matching_idxs = match_each_native_ptype!(arr.ptype(), |$T| {
        let rhs_typed: $T = rhs.try_into().unwrap();
        let predicate_fn = get_predicate::<$T>(&predicate.op);
        apply_predicate(arr.typed_data::<$T>(), &rhs_typed, predicate_fn)
    });

    Ok(matching_idxs.bitand(&present_buf))
}

fn apply_predicate<T: NativePType, F: Fn(&T, &T) -> bool>(
    lhs: &[T],
    rhs: &T,
    f: F,
) -> BooleanBuffer {
    let matches = lhs.iter().map(|lhs| f(lhs, rhs));
    BooleanBuffer::from_iter(matches)
}

fn get_predicate<T: NativePType>(op: &Operator) -> fn(&T, &T) -> bool {
    match op {
        Operator::EqualTo => PartialEq::eq,
        Operator::NotEqualTo => PartialEq::ne,
        Operator::GreaterThan => PartialOrd::gt,
        Operator::GreaterThanOrEqualTo => PartialOrd::ge,
        Operator::LessThan => PartialOrd::lt,
        Operator::LessThanOrEqualTo => PartialOrd::le,
    }
}

/// Merge an arbitrary number of bitsets
enum BooleanBufferMergeOp {
    Any,
    All,
}

impl BooleanBufferMergeOp {
    fn merge(self, bitsets: &mut dyn Iterator<Item = BooleanBuffer>) -> Option<BooleanBuffer> {
        match self {
            BooleanBufferMergeOp::Any => bitsets.tree_fold1(|a, b| a.bitor(&b)),
            BooleanBufferMergeOp::All => bitsets.tree_fold1(|a, b| a.bitand(&b)),
        }
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::field_paths::FieldPathBuilder;
    use vortex_expr::expressions::{lit, Conjunction};
    use vortex_expr::field_paths::FieldPathOperations;

    use super::*;
    use crate::validity::Validity;

    fn apply_conjunctive_filter(arr: &PrimitiveArray, conj: Conjunction) -> VortexResult<Array> {
        arr.filter_indices(&Disjunction {
            conjunctions: vec![conj],
        })
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

        let field = FieldPathBuilder::new().build();
        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().lt(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_primitive()
        .unwrap();
        let filtered = filtered_primitive.typed_data::<u64>();
        assert_eq!(filtered, [0u64, 1, 2, 3]);

        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().gt(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_primitive()
        .unwrap();
        let filtered = filtered_primitive.typed_data::<u64>();
        assert_eq!(filtered, [6u64, 7, 8, 10]);

        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().eq(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_primitive()
        .unwrap();
        let filtered = filtered_primitive.typed_data::<u64>();
        assert_eq!(filtered, [5]);

        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().gte(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_primitive()
        .unwrap();
        let filtered = filtered_primitive.typed_data::<u64>();
        assert_eq!(filtered, [5u64, 6, 7, 8, 10]);

        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().lte(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_primitive()
        .unwrap();
        let filtered = filtered_primitive.typed_data::<u64>();
        assert_eq!(filtered, [0u64, 1, 2, 3, 5]);
    }

    #[test]
    fn test_multiple_predicates() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPathBuilder::new().build();
        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().lt(lit(5u32)), field.clone().gt(lit(2u32))],
            },
        )
        .unwrap()
        .flatten_primitive()
        .unwrap();
        let filtered = filtered_primitive.typed_data::<u64>();
        assert_eq!(filtered, [2, 3])
    }

    #[test]
    fn test_disjoint_predicates() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPathBuilder::new().build();
        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().lt(lit(5u32)), field.clone().gt(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_primitive()
        .unwrap();
        let filtered = filtered_primitive.typed_data::<u64>();
        let expected: [u64; 0] = [];
        assert_eq!(filtered, expected)
    }

    #[test]
    fn test_disjunctive_predicate() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPathBuilder::new().build();
        let c1 = Conjunction {
            predicates: vec![field.clone().lt(lit(5u32))],
        };
        let c2 = Conjunction {
            predicates: vec![field.clone().gt(lit(5u32))],
        };

        let disj = Disjunction {
            conjunctions: vec![c1, c2],
        };
        let filtered_primitive = arr
            .filter_indices(&disj)
            .unwrap()
            .flatten_primitive()
            .unwrap();
        let filtered = filtered_primitive.typed_data::<u64>();
        assert_eq!(filtered, [0u64, 1, 2, 3, 5, 6, 7, 8, 9])
    }
}
