use vortex_error::VortexResult;

use crate::array::sparse::SparseArray;
use crate::compute::{search_sorted, SearchSortedSide};
use crate::compute::{slice, SliceFn};
use crate::{Array, IntoArray};

impl SliceFn for SparseArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        // Find the index of the first patch index that is greater than or equal to the offset of this array
        let index_start_index =
            search_sorted(&self.indices(), start, SearchSortedSide::Left)?.to_index();
        let index_end_index =
            search_sorted(&self.indices(), stop, SearchSortedSide::Left)?.to_index();

        Ok(Self::try_new_with_offset(
            slice(&self.indices(), index_start_index, index_end_index)?,
            slice(&self.values(), index_start_index, index_end_index)?,
            stop - start,
            self.indices_offset() + start,
            self.fill_value().clone(),
        )?
        .into_array())
    }
}
