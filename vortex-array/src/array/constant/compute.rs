use std::cmp::Ordering;
use std::sync::Arc;

use arrow_array::Datum;
use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_bail, VortexResult};
use vortex_expr::Operator;
use vortex_scalar::Scalar;

use crate::array::constant::ConstantArray;
use crate::arrow::FromArrowArray;
use crate::compute::unary::scalar_at::ScalarAtFn;
use crate::compute::{
    AndFn, ArrayCompute, CompareFn, OrFn, SearchResult, SearchSortedFn, SearchSortedSide, SliceFn,
    TakeFn,
};
use crate::{Array, ArrayDType, ArrayData, IntoArray, IntoArrayVariant, IntoCanonical};

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
        if self.dtype().eq_ignore_nullability(rhs.dtype()) && self.len() == rhs.len() {
            if let Ok(rhs) = ConstantArray::try_from(rhs) {
                let lhs = self.scalar();
                let rhs = rhs.scalar();

                let scalar = scalar_cmp(lhs, rhs, operator);

                return Ok(ConstantArray::new(scalar, self.len()).into_array());
            }

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
        } else {
            Ok(ConstantArray::new(false, rhs.len()).into_array())
        }
    }
}

impl AndFn for ConstantArray {
    fn and(&self, array: &Array) -> VortexResult<Array> {
        constant_array_bool_impl(self, array, |(l, r)| l & r)
    }
}

impl OrFn for ConstantArray {
    fn or(&self, array: &Array) -> VortexResult<Array> {
        constant_array_bool_impl(self, array, |(l, r)| l | r)
    }
}

fn scalar_cmp(lhs: &Scalar, rhs: &Scalar, operator: Operator) -> Scalar {
    if lhs.is_null() | rhs.is_null() {
        Scalar::null(DType::Bool(Nullability::Nullable))
    } else {
        let b = match operator {
            Operator::Eq => lhs == rhs,
            Operator::NotEq => lhs != rhs,
            Operator::Gt => lhs > rhs,
            Operator::Gte => lhs >= rhs,
            Operator::Lt => lhs < rhs,
            Operator::Lte => lhs <= rhs,
        };

        Scalar::bool(b, Nullability::Nullable)
    }
}

fn constant_array_bool_impl(
    constant_array: &ConstantArray,
    other: &Array,
    bool_op: impl Fn((bool, bool)) -> bool,
) -> VortexResult<Array> {
    if constant_array.dtype().is_boolean()
        && other.dtype().is_boolean()
        && constant_array.len() == other.len()
    {
        if let Ok(array) = ConstantArray::try_from(other.clone()) {
            let lhs = constant_array.scalar().value().as_bool()?;
            let rhs = array.scalar().value().as_bool()?;

            let scalar = match lhs.zip(rhs).map(bool_op) {
                Some(b) => Scalar::bool(b, Nullability::Nullable),
                None => Scalar::null(constant_array.dtype().as_nullable()),
            };

            Ok(ConstantArray::new(scalar, constant_array.len()).into_array())
        } else {
            AndFn::and(&constant_array.clone().into_bool()?, other)
        }
    } else {
        vortex_bail!("Boolean operations aren't supported on arrays of different lengths")
    }
}

#[cfg(test)]
mod test {
    use crate::array::constant::ConstantArray;
    use crate::compute::{search_sorted, SearchResult, SearchSortedSide};
    use crate::IntoArray;

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
}
