use crate::array::sparse::SparseArray;
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::search_sorted::{search_sorted_usize, SearchSortedSide};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::{NullableScalar, Scalar};

impl ArrayCompute for SparseArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for SparseArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        // Check whether `true_patch_index` exists in the patch index array
        // First, get the index of the patch index array that is the first index
        // greater than or equal to the true index
        let true_patch_index = index + self.indices_offset;
        search_sorted_usize(self.indices(), true_patch_index, SearchSortedSide::Left).and_then(
            |idx| {
                // If the value at this index is equal to the true index, then it exists in the patch index array
                // and we should return the value at the corresponding index in the patch values array
                scalar_at(self.indices(), idx)
                    .or_else(|_| Ok(NullableScalar::none(self.values().dtype().clone()).boxed()))
                    .and_then(usize::try_from)
                    .and_then(|patch_index| {
                        if patch_index == true_patch_index {
                            scalar_at(self.values(), idx)
                        } else {
                            Ok(NullableScalar::none(self.values().dtype().clone()).boxed())
                        }
                    })
            },
        )
    }
}
