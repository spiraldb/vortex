use vortex_error::VortexResult;

use crate::array::sparse::SparseArray;
use crate::compute::search_sorted::{search_sorted, SearchSortedSide};
use crate::compute::slice::{slice, SliceFn};
use crate::{IntoArray, OwnedArray};

impl SliceFn for SparseArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        // Find the index of the first patch index that is greater than or equal to the offset of this array
        let index_start_index =
            search_sorted(&self.indices(), start, SearchSortedSide::Left)?.to_index();
        let index_end_index =
            search_sorted(&self.indices(), stop, SearchSortedSide::Left)?.to_index();

        Ok(SparseArray::try_new_with_offset(
            slice(&self.indices(), index_start_index, index_end_index)?,
            slice(&self.values(), index_start_index, index_end_index)?,
            stop - start,
            self.indices_offset() + start,
            self.fill_value().clone(),
        )?
        .into_array())
    }
}
