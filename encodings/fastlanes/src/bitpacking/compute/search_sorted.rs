use std::cmp::Ordering;

use fastlanes::BitPacking;
use vortex::compute::search_sorted::{
    search_sorted, IndexOrd, Len, SearchResult, SearchSorted, SearchSortedFn, SearchSortedSide,
};
use vortex::{ArrayDType, IntoArrayVariant};
use vortex_dtype::{match_each_unsigned_integer_ptype, NativePType};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::{unpack_single_primitive, BitPackedArray};

impl SearchSortedFn for BitPackedArray {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<SearchResult> {
        let ptype = self.ptype();
        match_each_unsigned_integer_ptype!(ptype, |$P| {
            let unwrapped_value: $P = value.cast(self.dtype())?.try_into().unwrap();
            if let Some(patches_array) = self.patches() {
                if (unwrapped_value.leading_zeros() as usize) < ptype.bit_width() - self.bit_width() {
                    search_sorted(&patches_array, value.clone(), side)
                } else {
                    Ok(SearchSorted::search_sorted(&BitPackedSearch(self), &unwrapped_value, side))
                }
            } else {
                Ok(SearchSorted::search_sorted(&BitPackedSearch(self), &unwrapped_value, side))
            }
        })
    }
}

/// This wrapper exists, so that you can't invoke SearchSorted::search_sorted directly on BitPackedArray as it omits searching patches
#[derive(Debug)]
struct BitPackedSearch<'a>(&'a BitPackedArray);

impl<T: BitPacking + NativePType> IndexOrd<T> for BitPackedSearch<'_> {
    fn index_cmp(&self, idx: usize, elem: &T) -> Option<Ordering> {
        // SAFETY: Used in search_sorted_by which ensures that idx is within bounds
        let val: T = unsafe {
            unpack_single_primitive(
                self.0
                    .packed()
                    .into_primitive()
                    .unwrap()
                    .maybe_null_slice::<T>(),
                self.0.bit_width(),
                idx,
            )
            .unwrap()
        };
        val.partial_cmp(elem)
    }
}

impl Len for BitPackedSearch<'_> {
    fn len(&self) -> usize {
        self.0.metadata().length
    }
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::search_sorted::{search_sorted, SearchResult, SearchSortedSide};
    use vortex::IntoArray;

    use crate::BitPackedArray;

    #[test]
    fn search_with_patches() {
        let bitpacked = BitPackedArray::encode(
            &PrimitiveArray::from(vec![1u32, 2, 3, 4, 5]).into_array(),
            2,
        )
        .unwrap()
        .into_array();
        assert_eq!(
            search_sorted(&bitpacked, 4, SearchSortedSide::Left).unwrap(),
            SearchResult::Found(3)
        );
        assert_eq!(
            search_sorted(&bitpacked, 5, SearchSortedSide::Left).unwrap(),
            SearchResult::Found(4)
        );
        assert_eq!(
            search_sorted(&bitpacked, 6, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(5)
        );
        assert_eq!(
            search_sorted(&bitpacked, 0, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(0)
        );
    }
}
