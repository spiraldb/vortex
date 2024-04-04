use vortex_error::VortexResult;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::compute::search_sorted::SearchSorted;
use crate::compute::search_sorted::{SearchSortedFn, SearchSortedSide};
use crate::ptype::NativePType;
use crate::scalar::Scalar;

impl<T: NativePType> SearchSortedFn for &dyn PrimitiveTrait<T> {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<usize> {
        let pvalue: T = value.try_into()?;
        Ok(self.typed_data().search_sorted(&pvalue, side))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::array::IntoArray;
    use crate::compute::search_sorted::search_sorted;

    #[test]
    fn test_searchsorted_primitive() {
        let values = vec![1u16, 2, 3].into_array();

        assert_eq!(
            search_sorted(&values, 0, SearchSortedSide::Left).unwrap(),
            0
        );
        assert_eq!(
            search_sorted(&values, 1, SearchSortedSide::Left).unwrap(),
            0
        );
        assert_eq!(
            search_sorted(&values, 1, SearchSortedSide::Right).unwrap(),
            1
        );
        assert_eq!(
            search_sorted(&values, 4, SearchSortedSide::Left).unwrap(),
            3
        );
    }
}
