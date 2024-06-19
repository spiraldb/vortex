use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::sparse::SparseArray;
use crate::compute::search_sorted::{
    search_sorted, SearchResult, SearchSortedFn, SearchSortedSide,
};
use crate::compute::slice::SliceFn;
use crate::compute::take::TakeFn;
use crate::compute::unary::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::ArrayCompute;
use crate::ArrayDType;

mod slice;
mod take;

impl ArrayCompute for SparseArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for SparseArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match self.find_index(index)? {
            None => self.fill_value().clone().cast(self.dtype()),
            Some(idx) => scalar_at(&self.values(), idx)?.cast(self.dtype()),
        }
    }
}

impl SearchSortedFn for SparseArray {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<SearchResult> {
        search_sorted(&self.values(), value.clone(), side).and_then(|sr| match sr {
            SearchResult::Found(i) => {
                let index: usize = scalar_at(&self.indices(), i)?.as_ref().try_into().unwrap();
                Ok(SearchResult::Found(index))
            }
            SearchResult::NotFound(i) => {
                let index: usize = scalar_at(&self.indices(), if i == 0 { 0 } else { i - 1 })?
                    .as_ref()
                    .try_into()
                    .unwrap();
                Ok(SearchResult::NotFound(if i == 0 {
                    index
                } else {
                    index + 1
                }))
            }
        })
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::{DType, Nullability, PType};
    use vortex_scalar::Scalar;

    use crate::array::primitive::PrimitiveArray;
    use crate::array::sparse::SparseArray;
    use crate::compute::search_sorted::{search_sorted, SearchResult, SearchSortedSide};
    use crate::validity::Validity;
    use crate::{Array, IntoArray};

    fn array() -> Array {
        SparseArray::try_new(
            PrimitiveArray::from(vec![2u64, 9, 15]).into_array(),
            PrimitiveArray::from_vec(vec![33, 44, 55], Validity::AllValid).into_array(),
            20,
            Scalar::null(DType::Primitive(PType::I32, Nullability::Nullable)),
        )
        .unwrap()
        .into_array()
    }

    #[test]
    pub fn search_larger_than() {
        let res = search_sorted(&array(), 66, SearchSortedSide::Left).unwrap();
        assert_eq!(res, SearchResult::NotFound(16));
    }

    #[test]
    pub fn search_less_than() {
        let res = search_sorted(&array(), 22, SearchSortedSide::Left).unwrap();
        assert_eq!(res, SearchResult::NotFound(2));
    }

    #[test]
    pub fn search_found() {
        let res = search_sorted(&array(), 44, SearchSortedSide::Left).unwrap();
        assert_eq!(res, SearchResult::Found(9));
    }
}
