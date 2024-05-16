use croaring::Bitmap;
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
            BitmapMergeOp::All(
                &mut conj
                    .predicates
                    .iter()
                    .map(|pred| indices_matching_predicate(self, pred).unwrap()),
            )
            .merge()
        });
        let indices = BitmapMergeOp::Any(&mut conjunction_indices)
            .merge()
            .map(|bitmap| bitmap.iter().map(|idx| idx as u64).collect_vec())
            .unwrap_or(Vec::new());
        Ok(PrimitiveArray::from_vec(indices, Validity::AllValid).into_array())
    }
}

fn indices_matching_predicate(arr: &PrimitiveArray, predicate: &Predicate) -> VortexResult<Bitmap> {
    if predicate.left.head().is_some() {
        vortex_bail!("Invalid path for primitive array")
    }
    let validity = arr.validity();
    let rhs = match &predicate.right {
        Value::Field(_) => {
            vortex_bail!("Right-hand-side fields not yet supported.")
        }
        Value::Literal(scalar) => scalar,
    };

    let matching_idxs: Vec<u32> = match_each_native_ptype!(arr.ptype(), |$T| {
        let rhs_typed: $T = rhs.try_into().unwrap();
        let predicate_fn = get_predicate::<$T>(&predicate.op);

        arr.typed_data::<$T>().iter().enumerate().filter(|(idx, &v)| {
            predicate_fn(&v, &rhs_typed)
        })
        .filter(|(idx, _)| validity.is_valid(idx.clone()))
        //todo(@jcasale): 64-bit RoaringBitmap?
        .map(|(idx, _)| idx as u32)
        .collect_vec()
    });
    //todo(@jcasale): 64-bit RoaringBitmap?
    let mut bitmap = Bitmap::with_container_capacity(arr.len() as u32);

    matching_idxs.into_iter().for_each(|idx| bitmap.add(idx));

    Ok(bitmap)
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

/// Merge an arbitrary number of bitmaps
enum BitmapMergeOp<'a> {
    Any(&'a mut dyn Iterator<Item = Bitmap>),
    All(&'a mut dyn Iterator<Item = Bitmap>),
}

impl BitmapMergeOp<'_> {
    fn merge(self) -> Option<Bitmap> {
        match self {
            BitmapMergeOp::Any(bitmaps) => bitmaps.reduce(|a, b| a.or(&b)),
            BitmapMergeOp::All(bitmaps) => bitmaps.reduce(|a, b| a.and(&b)),
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
        let arr = PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9], Validity::AllValid);

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
        assert_eq!(filtered, [5u64, 6, 7, 8]);

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
        assert_eq!(filtered, [4]);

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
        assert_eq!(filtered, [4u64, 5, 6, 7, 8]);

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
        assert_eq!(filtered, [0u64, 1, 2, 3, 4]);
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
