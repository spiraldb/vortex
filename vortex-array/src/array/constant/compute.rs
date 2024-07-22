use std::cmp::Ordering;
use std::sync::Arc;

use vortex_dtype::Nullability;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::array::constant::ConstantArray;
use crate::compute::unary::{scalar_at, ScalarAtFn};
use crate::compute::{
    AndFn, ArrayCompute, OrFn, SearchResult, SearchSortedFn, SearchSortedSide, SliceFn, TakeFn,
};
use crate::stats::{ArrayStatistics, Stat};
use crate::{Array, ArrayDType, AsArray, IntoArray};

impl ArrayCompute for ConstantArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }

    fn compare(&self) -> Option<&dyn CompareFn> {
        Some(self)
    }

    fn and(&self) -> Option<&dyn AndFn> {
        Some(self)
    }

    fn or(&self) -> Option<&dyn OrFn> {
        Some(self)
    }
}

impl ScalarAtFn for ConstantArray {
    fn scalar_at(&self, _index: usize) -> VortexResult<Scalar> {
        Ok(self.scalar().clone())
    }
}

impl TakeFn for ConstantArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        Ok(Self::new(self.scalar().clone(), indices.len()).into_array())
    }
}

impl SliceFn for ConstantArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::new(self.scalar().clone(), stop - start).into_array())
    }
}

impl SearchSortedFn for ConstantArray {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<SearchResult> {
        match self.scalar().partial_cmp(value).unwrap_or(Ordering::Less) {
            Ordering::Greater => Ok(SearchResult::NotFound(0)),
            Ordering::Less => Ok(SearchResult::NotFound(self.len())),
            Ordering::Equal => match side {
                SearchSortedSide::Left => Ok(SearchResult::Found(0)),
                SearchSortedSide::Right => Ok(SearchResult::Found(self.len())),
            },
        }
    }
}

impl CompareFn for ConstantArray {
    fn compare(&self, rhs: &Array, operator: Operator) -> VortexResult<Array> {
        if let Some(true) = rhs.statistics().compute_is_constant() {
            let lhs = self.scalar();
            let rhs = scalar_at(rhs, 0)?;

            let scalar = scalar_cmp(lhs, &rhs, operator);

            Ok(ConstantArray::new(scalar, self.len()).into_array())
        } else {
            let datum = Arc::<dyn Datum>::from(self.scalar().clone());
            let rhs = rhs.clone().into_canonical()?.into_arrow();
            let rhs = rhs.as_ref();

            let boolean_array = match operator {
                Operator::Eq => arrow_ord::cmp::eq(datum.as_ref(), &rhs)?,
                Operator::NotEq => arrow_ord::cmp::neq(datum.as_ref(), &rhs)?,
                Operator::Gt => arrow_ord::cmp::gt(datum.as_ref(), &rhs)?,
                Operator::Gte => arrow_ord::cmp::gt_eq(datum.as_ref(), &rhs)?,
                Operator::Lt => arrow_ord::cmp::lt(datum.as_ref(), &rhs)?,
                Operator::Lte => arrow_ord::cmp::lt_eq(datum.as_ref(), &rhs)?,
            };

            Ok(ArrayData::from_arrow(&boolean_array, true).into_array())
        }
    }
}


impl AndFn for ConstantArray {
    fn and(&self, array: &Array) -> VortexResult<Array> {
        constant_array_bool_impl(
            self,
            array,
            |(l, r)| l & r,
            |other, this| other.with_dyn(|other| other.and().map(|other| other.and(this))),
        )
    }
}

impl OrFn for ConstantArray {
    fn or(&self, array: &Array) -> VortexResult<Array> {
        constant_array_bool_impl(
            self,
            array,
            |(l, r)| l | r,
            |other, this| other.with_dyn(|other| other.or().map(|other| other.or(this))),
        )
    }
}

fn constant_array_bool_impl(
    constant_array: &ConstantArray,
    other: &Array,
    bool_op: impl Fn((bool, bool)) -> bool,
    fallback_fn: impl Fn(&Array, &Array) -> Option<VortexResult<Array>>,
) -> VortexResult<Array> {
    // If the right side is constant
    if let Some(true) = other.statistics().get_as::<bool>(Stat::IsConstant) {
        let lhs = constant_array.scalar().value().as_bool()?;
        let rhs = scalar_at(other, 0)?.value().as_bool()?;

        let scalar = match lhs.zip(rhs).map(bool_op) {
            Some(b) => Scalar::bool(b, Nullability::Nullable),
            None => Scalar::null(constant_array.dtype().as_nullable()),
        };

        Ok(ConstantArray::new(scalar, constant_array.len()).into_array())
    } else {
        // try and use a the rhs specialized implementation if it exists
        match fallback_fn(other, constant_array.as_array_ref()) {
            Some(r) => r,
            None => vortex_bail!("Operation is not supported"),
        }
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use crate::array::bool::BoolArray;
    use crate::array::constant::ConstantArray;
    use crate::compute::unary::scalar_at;
    use crate::compute::{and, or, search_sorted, SearchResult, SearchSortedSide};
    use crate::{Array, IntoArray, IntoArrayVariant};

    #[test]
    pub fn search() {
        let cst = ConstantArray::new(42, 5000).into_array();
        assert_eq!(
            search_sorted(&cst, 33, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(0)
        );
        assert_eq!(
            search_sorted(&cst, 55, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(5000)
        );
    }

    #[test]
    pub fn search_equals() {
        let cst = ConstantArray::new(42, 5000).into_array();
        assert_eq!(
            search_sorted(&cst, 42, SearchSortedSide::Left).unwrap(),
            SearchResult::Found(0)
        );
        assert_eq!(
            search_sorted(&cst, 42, SearchSortedSide::Right).unwrap(),
            SearchResult::Found(5000)
        );
    }

    #[rstest]
    #[case(ConstantArray::new(true, 4).into_array(), BoolArray::from_iter([Some(true), Some(false), Some(true), Some(false)].into_iter()).into_array())]
    #[case(BoolArray::from_iter([Some(true), Some(false), Some(true), Some(false)].into_iter()).into_array(), ConstantArray::new(true, 4).into_array())]
    fn test_or(#[case] lhs: Array, #[case] rhs: Array) {
        let r = or(&lhs, &rhs).unwrap().into_bool().unwrap().into_array();

        let v0 = scalar_at(&r, 0).unwrap().value().as_bool().unwrap();
        let v1 = scalar_at(&r, 1).unwrap().value().as_bool().unwrap();
        let v2 = scalar_at(&r, 2).unwrap().value().as_bool().unwrap();
        let v3 = scalar_at(&r, 3).unwrap().value().as_bool().unwrap();

        assert!(v0.unwrap());
        assert!(v1.unwrap());
        assert!(v2.unwrap());
        assert!(v3.unwrap());
    }

    #[rstest]
    #[case(ConstantArray::new(true, 4).into_array(), BoolArray::from_iter([Some(true), Some(false), Some(true), Some(false)].into_iter()).into_array())]
    #[case(BoolArray::from_iter([Some(true), Some(false), Some(true), Some(false)].into_iter()).into_array(),
        ConstantArray::new(true, 4).into_array())]
    fn test_and(#[case] lhs: Array, #[case] rhs: Array) {
        let r = and(&lhs, &rhs).unwrap().into_bool().unwrap().into_array();

        let v0 = scalar_at(&r, 0).unwrap().value().as_bool().unwrap();
        let v1 = scalar_at(&r, 1).unwrap().value().as_bool().unwrap();
        let v2 = scalar_at(&r, 2).unwrap().value().as_bool().unwrap();
        let v3 = scalar_at(&r, 3).unwrap().value().as_bool().unwrap();

        assert!(v0.unwrap());
        assert!(!v1.unwrap());
        assert!(v2.unwrap());
        assert!(!v3.unwrap());
    }
}
