use std::cmp::Ordering;
use std::cmp::Ordering::Greater;

use fastlanes::BitPacking;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::compute::search_sorted::{
    search_sorted, IndexOrd, Len, SearchResult, SearchSorted, SearchSortedFn, SearchSortedSide,
};
use vortex::{ArrayDType, ArrayTrait, IntoArrayVariant};
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
                    Ok(SearchSorted::search_sorted(&BitPackedSearch::new(self), &unwrapped_value, side))
                }
            } else {
                Ok(SearchSorted::search_sorted(&BitPackedSearch::new(self), &unwrapped_value, side))
            }
        })
    }
}

/// This wrapper exists, so that you can't invoke SearchSorted::search_sorted directly on BitPackedArray as it omits searching patches
#[derive(Debug)]
struct BitPackedSearch {
    packed: PrimitiveArray,
    length: usize,
    bit_width: usize,
    min_patch_offset: Option<usize>,
}

impl BitPackedSearch {
    pub fn new(array: &BitPackedArray) -> Self {
        Self {
            packed: array.packed().flatten_primitive().unwrap(),
            length: array.len(),
            bit_width: array.bit_width(),
            min_patch_offset: array.patches().map(|p| {
                SparseArray::try_from(p)
                    .expect("Only Sparse patches are supported")
                    .min_index()
            }),
        }
    }
}

impl<T: BitPacking + NativePType> IndexOrd<T> for BitPackedSearch {
    fn index_cmp(&self, idx: usize, elem: &T) -> Option<Ordering> {
        if let Some(min_patch) = self.min_patch_offset {
            if idx >= min_patch {
                return Some(Greater);
            }
        }
        // SAFETY: Used in search_sorted_by which ensures that idx is within bounds
        let val: T = unsafe {
            unpack_single_primitive(self.packed.maybe_null_slice::<T>(), self.bit_width, idx)
                .unwrap()
        };
        val.partial_cmp(elem)
    }
}

impl Len for BitPackedSearch {
    fn len(&self) -> usize {
        self.length
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
