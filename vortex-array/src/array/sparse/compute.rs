use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::sparse::SparseArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::search_sorted::{search_sorted_usize, SearchSortedSide};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::{NullableScalar, Scalar, ScalarRef};
use itertools::Itertools;

impl ArrayCompute for SparseArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl AsContiguousFn for SparseArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        Ok(SparseArray::new(
            as_contiguous(
                arrays
                    .iter()
                    .map(|a| a.as_sparse().indices())
                    .map(|a| dyn_clone::clone_box(a))
                    .collect_vec(),
            )?,
            as_contiguous(
                arrays
                    .iter()
                    .map(|a| a.as_sparse().values())
                    .map(|a| dyn_clone::clone_box(a))
                    .collect_vec(),
            )?,
            arrays.iter().map(|a| a.len()).sum(),
        )
        .boxed())
    }
}

impl ScalarAtFn for SparseArray {
    fn scalar_at(&self, index: usize) -> VortexResult<ScalarRef> {
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
