use vortex_error::VortexResult;

use crate::array::sparse::SparseArray;
use crate::compute::{search_sorted, slice, SearchSortedSide, SliceFn};
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

#[cfg(test)]
mod tests {
    use vortex_dtype::Nullability;
    use vortex_scalar::Scalar;

    use super::*;

    #[test]
    fn test_slice() {
        let values = vec![15_u32, 135, 13531, 42].into_array();
        let indices = vec![10_u64, 11, 50, 100].into_array();

        let sparse = SparseArray::try_new(
            indices.clone(),
            values,
            101,
            Scalar::primitive(0_u32, Nullability::NonNullable),
        )
        .unwrap()
        .into_array();

        let sliced = slice(&sparse, 15, 100).unwrap();
        assert_eq!(sliced.len(), 100 - 15);
        let primitive = SparseArray::try_from(sliced)
            .unwrap()
            .values()
            .as_primitive();

        let sliced_values = primitive.maybe_null_slice::<u32>();

        assert_eq!(sliced_values, &[13531]);
    }
}
