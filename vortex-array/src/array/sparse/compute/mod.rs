use vortex_dtype::match_each_integer_ptype;
use vortex_error::{VortexExpect, VortexResult, VortexUnwrap as _};
use vortex_scalar::Scalar;

use crate::array::sparse::SparseArray;
use crate::array::PrimitiveArray;
use crate::compute::unary::{scalar_at, scalar_at_unchecked, ScalarAtFn};
use crate::compute::{
    search_sorted, take, ArrayCompute, FilterFn, SearchResult, SearchSortedFn, SearchSortedSide,
    SliceFn, TakeFn,
};
use crate::{Array, ArrayDType, IntoArray, IntoArrayVariant};

mod slice;
mod take;

impl ArrayCompute for SparseArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }

    fn filter(&self) -> Option<&dyn FilterFn> {
        Some(self)
    }
}

impl ScalarAtFn for SparseArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match self.search_index(index)?.to_found() {
            None => self.fill_value().clone().cast(self.dtype()),
            Some(idx) => scalar_at_unchecked(&self.values(), idx).cast(self.dtype()),
        }
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        match self.search_index(index).vortex_unwrap().to_found() {
            None => self.fill_value().clone().cast(self.dtype()).vortex_unwrap(),
            Some(idx) => scalar_at_unchecked(&self.values(), idx)
                .cast(self.dtype())
                .vortex_unwrap(),
        }
    }
}

impl SearchSortedFn for SparseArray {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<SearchResult> {
        search_sorted(&self.values(), value.clone(), side).and_then(|sr| {
            let sidx = sr.to_offset_ends_index(self.metadata().indices_len);
            let index: usize = scalar_at(&self.indices(), sidx)?.as_ref().try_into()?;
            Ok(match sr {
                SearchResult::Found(i) => SearchResult::Found(
                    if i == self.metadata().indices_len {
                        index + 1
                    } else {
                        index
                    } - self.indices_offset(),
                ),
                SearchResult::NotFound(i) => SearchResult::NotFound(
                    if i == 0 { index } else { index + 1 } - self.indices_offset(),
                ),
            })
        })
    }
}

impl FilterFn for SparseArray {
    fn filter(&self, predicate: &Array) -> VortexResult<Array> {
        let predicate = predicate.clone().into_bool()?;
        let buffer = predicate.boolean_buffer();
        let mut coordinate_indices = Vec::new();
        let mut value_indices = Vec::new();
        let mut last_inserted_index = 0;

        let flat_indices = self
            .indices()
            .into_primitive()
            .vortex_expect("Failed to convert SparseArray indices to primitive array");
        match_each_integer_ptype!(flat_indices.ptype(), |$P| {
            let indices = flat_indices
                .maybe_null_slice::<$P>()
                .iter()
                .map(|v| (*v as usize) - self.indices_offset());
            for (value_idx, coordinate) in indices.enumerate() {
                if buffer.value(coordinate) {
                    // We count the number of truthy values between this coordinate and the previous truthy one
                    let adjusted_coordinate = buffer.slice(last_inserted_index, coordinate - last_inserted_index).count_set_bits() as u64;
                    coordinate_indices.push(adjusted_coordinate);
                    last_inserted_index = coordinate;

                    value_indices.push(value_idx as u64);
                }
            }
        });

        Ok(SparseArray::try_new(
            PrimitiveArray::from(coordinate_indices).into_array(),
            take(&self.values(), PrimitiveArray::from(value_indices).array())?,
            buffer.count_set_bits(),
            self.fill_value().clone(),
        )?
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use rstest::{fixture, rstest};
    use vortex_dtype::{DType, Nullability, PType};
    use vortex_scalar::Scalar;

    use crate::array::primitive::PrimitiveArray;
    use crate::array::sparse::SparseArray;
    use crate::array::BoolArray;
    use crate::compute::{filter, search_sorted, slice, SearchResult, SearchSortedSide};
    use crate::validity::Validity;
    use crate::{Array, IntoArray};

    #[fixture]
    fn array() -> Array {
        SparseArray::try_new(
            PrimitiveArray::from(vec![2u64, 9, 15]).into_array(),
            PrimitiveArray::from_vec(vec![33_i32, 44, 55], Validity::AllValid).into_array(),
            20,
            Scalar::null(DType::Primitive(PType::I32, Nullability::Nullable)),
        )
        .unwrap()
        .into_array()
    }

    #[rstest]
    fn search_larger_than(array: Array) {
        let res = search_sorted(&array, 66, SearchSortedSide::Left).unwrap();
        assert_eq!(res, SearchResult::NotFound(16));
    }

    #[rstest]
    fn search_less_than(array: Array) {
        let res = search_sorted(&array, 22, SearchSortedSide::Left).unwrap();
        assert_eq!(res, SearchResult::NotFound(2));
    }

    #[rstest]
    fn search_found(array: Array) {
        let res = search_sorted(&array, 44, SearchSortedSide::Left).unwrap();
        assert_eq!(res, SearchResult::Found(9));
    }

    #[rstest]
    fn search_not_found_right(array: Array) {
        let res = search_sorted(&array, 56, SearchSortedSide::Right).unwrap();
        assert_eq!(res, SearchResult::NotFound(16));
    }

    #[rstest]
    fn search_sliced(array: Array) {
        let array = slice(&array, 7, 20).unwrap();
        assert_eq!(
            search_sorted(&array, 22, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(2)
        );
    }

    #[test]
    fn search_right() {
        let array = SparseArray::try_new(
            PrimitiveArray::from(vec![0u64]).into_array(),
            PrimitiveArray::from_vec(vec![0u8], Validity::AllValid).into_array(),
            2,
            Scalar::null(DType::Primitive(PType::U8, Nullability::Nullable)),
        )
        .unwrap()
        .into_array();

        assert_eq!(
            search_sorted(&array, 0, SearchSortedSide::Right).unwrap(),
            SearchResult::Found(1)
        );
        assert_eq!(
            search_sorted(&array, 1, SearchSortedSide::Right).unwrap(),
            SearchResult::NotFound(1)
        );
    }

    #[rstest]
    fn test_filter(array: Array) {
        let mut predicate = vec![false, false, true];
        predicate.extend_from_slice(&[false; 17]);
        let predicate = BoolArray::from_vec(predicate, Validity::NonNullable).into_array();

        let filtered_array = filter(&array, &predicate).unwrap();
        let filtered_array = SparseArray::try_from(filtered_array).unwrap();

        assert_eq!(filtered_array.len(), 1);
        assert_eq!(filtered_array.values().len(), 1);
        assert_eq!(filtered_array.indices().len(), 1);
    }
}
