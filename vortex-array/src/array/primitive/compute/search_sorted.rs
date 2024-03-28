use crate::array::Array;
use itertools::Itertools;
use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::cast::cast;
use crate::compute::flatten::flatten_primitive;
use crate::compute::search_sorted::{SearchSorted, SearchSortedManyFn};
use crate::compute::search_sorted::{SearchSortedFn, SearchSortedSide};
use crate::match_each_native_ptype;
use crate::scalar::Scalar;

impl SearchSortedFn for PrimitiveArray {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<usize> {
        // TODO(ngates): how should we handle nulls?
        match_each_native_ptype!(self.ptype(), |$T| {
            let pvalue: $T = value.try_into()?;
            Ok(self.typed_data::<$T>().search_sorted(&pvalue, side))
        })
    }
}

impl SearchSortedManyFn for PrimitiveArray {
    fn search_sorted_many(
        &self,
        values: &dyn Array,
        side: SearchSortedSide,
    ) -> VortexResult<Vec<usize>> {
        // TODO(ngates): use statistics to get a measure of uniqueness? We may benefit from
        //  memoizing the search_sorted results for the values array.
        let values = flatten_primitive(cast(values, self.dtype())?.as_ref())?;
        assert_eq!(self.ptype(), values.ptype());

        // Switch once on side, instead of once per value
        Ok(match_each_native_ptype!(self.ptype(), |$T| {
            match side {
                SearchSortedSide::Left => values.typed_data::<$T>()
                    .iter()
                    .map(|v| self.typed_data::<$T>().search_sorted_left(v))
                    .collect_vec(),
                SearchSortedSide::Right => values.typed_data::<$T>()
                    .iter()
                    .map(|v| self.typed_data::<$T>().search_sorted_right(v))
                    .collect_vec(),
            }
        }))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::array::IntoArray;
    use crate::compute::search_sorted::{search_sorted, search_sorted_many};

    #[test]
    fn test_search_sorted_primitive() {
        let values = vec![1u16, 2, 3].into_array();

        assert_eq!(search_sorted(&values, 0, SearchSortedSide::Left), Ok(0));
        assert_eq!(search_sorted(&values, 1, SearchSortedSide::Left), Ok(0));
        assert_eq!(search_sorted(&values, 1, SearchSortedSide::Right), Ok(1));
        assert_eq!(search_sorted(&values, 4, SearchSortedSide::Left), Ok(3));
    }

    #[test]
    fn test_search_sorted_many_primitive() {
        let values = vec![1u16, 2, 3].into_array();
        let targets = vec![0u16, 1, 4].into_array();

        assert_eq!(
            search_sorted_many(&values, &targets, SearchSortedSide::Left),
            Ok(vec![0, 0, 3])
        );
        assert_eq!(
            search_sorted_many(&values, &targets, SearchSortedSide::Right),
            Ok(vec![0, 0, 3])
        );
    }
}
