use std::cmp::Ordering;
use std::cmp::Ordering::Greater;

use vortex_dtype::{match_each_native_ptype, NativePType};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::primitive::PrimitiveArray;
use crate::compute::{IndexOrd, Len, SearchResult, SearchSorted, SearchSortedFn, SearchSortedSide};
use crate::validity::Validity;

impl SearchSortedFn for PrimitiveArray {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<SearchResult> {
        match_each_native_ptype!(self.ptype(), |$T| {
            match self.validity() {
                Validity::NonNullable | Validity::AllValid => {
                    let pvalue: $T = value.try_into()?;
                    Ok(self.maybe_null_slice::<$T>().search_sorted(&pvalue, side))
                }
                Validity::AllInvalid => Ok(SearchResult::NotFound(0)),
                Validity::Array(_) => {
                    let pvalue: $T = value.try_into()?;
                    Ok(NullableSearchSorted::new(self).search_sorted(&pvalue, side))
                }
            }
        })
    }
}

struct NullableSearchSorted<'a, T> {
    values: &'a [T],
    validity: Validity,
}

impl<'a, T: NativePType> NullableSearchSorted<'a, T> {
    pub fn new(array: &'a PrimitiveArray) -> Self {
        Self {
            values: array.maybe_null_slice(),
            validity: array.validity(),
        }
    }
}

impl<'a, T: NativePType> IndexOrd<T> for NullableSearchSorted<'a, T> {
    fn index_cmp(&self, idx: usize, elem: &T) -> Option<Ordering> {
        if self.validity.is_null(idx) {
            return Some(Greater);
        }

        self.values.index_cmp(idx, elem)
    }
}

impl<'a, T> Len for NullableSearchSorted<'a, T> {
    fn len(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::compute::search_sorted;
    use crate::IntoArray;

    #[test]
    fn test_searchsorted_primitive() {
        let values = vec![1u16, 2, 3].into_array();

        assert_eq!(
            search_sorted(&values, 0, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(0)
        );
        assert_eq!(
            search_sorted(&values, 1, SearchSortedSide::Left).unwrap(),
            SearchResult::Found(0)
        );
        assert_eq!(
            search_sorted(&values, 1, SearchSortedSide::Right).unwrap(),
            SearchResult::Found(1)
        );
        assert_eq!(
            search_sorted(&values, 4, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(3)
        );
    }
}
