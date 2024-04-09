use std::collections::HashMap;

use arrow_buffer::BooleanBufferBuilder;
use itertools::Itertools;
use vortex_error::{vortex_bail, VortexResult};

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::SparseArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{flatten_primitive, FlattenFn, FlattenedArray};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::search_sorted::{search_sorted, SearchSortedSide};
use crate::compute::take::{take, TakeFn};
use crate::compute::ArrayCompute;
use crate::ptype::NativePType;
use crate::scalar::Scalar;
use crate::{match_each_integer_ptype, match_each_native_ptype};

impl ArrayCompute for SparseArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl AsContiguousFn for SparseArray {
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
        let all_fill_types_are_equal = arrays
            .iter()
            .map(|a| a.as_sparse().fill_value())
            .all_equal();
        if !all_fill_types_are_equal {
            vortex_bail!("Cannot concatenate SparseArrays with differing fill values");
        }

        Ok(SparseArray::new(
            as_contiguous(
                &arrays
                    .iter()
                    .map(|a| a.as_sparse().indices())
                    .cloned()
                    .collect_vec(),
            )?,
            as_contiguous(
                &arrays
                    .iter()
                    .map(|a| a.as_sparse().values())
                    .cloned()
                    .collect_vec(),
            )?,
            arrays.iter().map(|a| a.len()).sum(),
            self.fill_value().clone(),
        )
        .to_array_data())
    }
}

impl FlattenFn for SparseArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        // Resolve our indices into a vector of usize applying the offset
        let indices = self.resolved_indices();

        let mut validity = BooleanBufferBuilder::new(self.len());
        validity.append_n(self.len(), false);
        let values = flatten_primitive(self.values())?;
        match_each_native_ptype!(values.ptype(), |$P| {
            flatten_sparse_values(
                values.typed_data::<$P>(),
                &indices,
                self.len(),
                self.fill_value(),
                validity
            )
        })
    }
}

fn flatten_sparse_values<T: NativePType>(
    values: &[T],
    indices: &[usize],
    len: usize,
    fill_value: &Scalar,
    mut validity: BooleanBufferBuilder,
) -> VortexResult<FlattenedArray> {
    let primitive_fill = if fill_value.is_null() {
        T::default()
    } else {
        fill_value.try_into()?
    };
    let mut result = vec![primitive_fill; len];

    for (v, idx) in values.iter().zip_eq(indices) {
        result[*idx] = *v;
        validity.set_bit(*idx, true);
    }

    let validity = validity.finish();
    let array = if fill_value.is_null() {
        PrimitiveArray::from_nullable(result, Some(validity.into()))
    } else {
        PrimitiveArray::from(result)
    };
    Ok(FlattenedArray::Primitive(array))
}

impl ScalarAtFn for SparseArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match self.find_index(index)? {
            None => self.fill_value().clone().cast(self.dtype()),
            Some(idx) => scalar_at(self.values(), idx)?.cast(self.dtype()),
        }
    }
}

impl TakeFn for SparseArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let flat_indices = flatten_primitive(indices)?;
        // if we are taking a lot of values we should build a hashmap
        let (positions, physical_take_indices) = if indices.len() > 128 {
            take_map(self, &flat_indices)?
        } else {
            take_search_sorted(self, &flat_indices)?
        };

        let taken_values = take(self.values(), &physical_take_indices)?;

        Ok(SparseArray::new(
            positions.to_array_data(),
            taken_values,
            indices.len(),
            self.fill_value().clone(),
        )
        .to_array_data())
    }
}

fn take_map(
    array: &SparseArray,
    indices: &PrimitiveArray,
) -> VortexResult<(PrimitiveArray, PrimitiveArray)> {
    let indices_map: HashMap<u64, u64> = array
        .resolved_indices()
        .iter()
        .enumerate()
        .map(|(i, r)| (*r as u64, i as u64))
        .collect();
    let (positions, patch_indices): (Vec<u64>, Vec<u64>) = match_each_integer_ptype!(indices.ptype(), |$P| {
        indices.typed_data::<$P>()
            .iter()
            .map(|pi| *pi as u64)
            .enumerate()
            .filter_map(|(i, pi)| indices_map.get(&pi).map(|phy_idx| (i as u64, phy_idx)))
            .unzip()
    });
    Ok((
        PrimitiveArray::from(positions),
        PrimitiveArray::from(patch_indices),
    ))
}

fn take_search_sorted(
    array: &SparseArray,
    indices: &PrimitiveArray,
) -> VortexResult<(PrimitiveArray, PrimitiveArray)> {
    // adjust the input indices (to take) by the internal index offset of the array
    let adjusted_indices = match_each_integer_ptype!(indices.ptype(), |$P| {
         indices.typed_data::<$P>()
            .iter()
            .map(|i| *i as usize + array.indices_offset())
            .collect::<Vec<_>>()
    });

    // TODO(robert): Use binary search instead of search_sorted + take and index validation to avoid extra work
    // search_sorted for the adjusted indices (need to validate that they are an exact match still)
    let physical_indices = adjusted_indices
        .iter()
        .map(|i| search_sorted(array.indices(), *i, SearchSortedSide::Left).map(|s| s as u64))
        .collect::<VortexResult<Vec<_>>>()?;

    // filter out indices that are out of bounds, which will cause the take to fail
    let (adjusted_indices, physical_indices): (Vec<usize>, Vec<u64>) = adjusted_indices
        .iter()
        .zip_eq(physical_indices)
        .filter(|(_, phys_idx)| *phys_idx < array.indices().len() as u64)
        .unzip();

    let physical_indices = PrimitiveArray::from(physical_indices);
    let taken_indices = flatten_primitive(&take(array.indices(), &physical_indices)?)?;
    let exact_matches: Vec<bool> = match_each_integer_ptype!(taken_indices.ptype(), |$P| {
        taken_indices
            .typed_data::<$P>()
            .iter()
            .zip_eq(adjusted_indices)
            .map(|(taken_idx, adj_idx)| *taken_idx as usize == adj_idx)
            .collect()
    });
    let (positions, patch_indices): (Vec<u64>, Vec<u64>) = physical_indices
        .typed_data::<u64>()
        .iter()
        .enumerate()
        .filter_map(|(i, phy_idx)| {
            // search_sorted != binary search, so we need to filter out indices that weren't found
            if exact_matches[i] {
                Some((i as u64, *phy_idx))
            } else {
                None
            }
        })
        .unzip();
    Ok((
        PrimitiveArray::from(positions),
        PrimitiveArray::from(patch_indices),
    ))
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use vortex_schema::{DType, FloatWidth, Nullability};

    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::sparse::compute::take_map;
    use crate::array::sparse::SparseArray;
    use crate::array::Array;
    use crate::compute::as_contiguous::as_contiguous;
    use crate::compute::take::take;
    use crate::scalar::Scalar;

    fn sparse_array() -> SparseArray {
        SparseArray::new(
            PrimitiveArray::from(vec![0u64, 37, 47, 99]).to_array_data(),
            PrimitiveArray::from(vec![1.23f64, 0.47, 9.99, 3.5]).to_array_data(),
            100,
            Scalar::null(&DType::Float(FloatWidth::_64, Nullability::Nullable)),
        )
    }

    #[test]
    fn sparse_take() {
        let sparse = sparse_array();
        let taken = take(&sparse, &PrimitiveArray::from(vec![0, 47, 47, 0, 99])).unwrap();
        assert_eq!(
            taken
                .as_sparse()
                .indices()
                .as_primitive()
                .typed_data::<u64>(),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(
            taken
                .as_sparse()
                .values()
                .as_primitive()
                .typed_data::<f64>(),
            [1.23f64, 9.99, 9.99, 1.23, 3.5]
        );
    }

    #[test]
    fn nonexistent_take() {
        let sparse = sparse_array();
        let taken = take(&sparse, &PrimitiveArray::from(vec![69])).unwrap();
        assert_eq!(
            taken
                .as_sparse()
                .indices()
                .as_primitive()
                .typed_data::<u64>(),
            []
        );
        assert_eq!(
            taken
                .as_sparse()
                .values()
                .as_primitive()
                .typed_data::<f64>(),
            []
        );
    }

    #[test]
    fn ordered_take() {
        let sparse = sparse_array();
        let taken = take(&sparse, &PrimitiveArray::from(vec![69, 37])).unwrap();
        assert_eq!(
            taken
                .as_sparse()
                .indices()
                .as_primitive()
                .typed_data::<u64>(),
            [1]
        );
        assert_eq!(
            taken
                .as_sparse()
                .values()
                .as_primitive()
                .typed_data::<f64>(),
            [0.47f64]
        );
        assert_eq!(taken.len(), 2);
    }

    #[test]
    fn take_slices_and_reassemble() {
        let sparse = sparse_array();
        let indices: PrimitiveArray = (0u64..10).collect_vec().into();
        let slices = (0..10)
            .map(|i| sparse.slice(i * 10, (i + 1) * 10).unwrap())
            .collect_vec();

        let taken = slices
            .iter()
            .map(|s| take(s, &indices).unwrap())
            .collect_vec();
        for i in [1, 2, 5, 6, 7, 8] {
            assert_eq!(taken[i].as_sparse().indices().len(), 0);
        }
        for i in [0, 3, 4, 9] {
            assert_eq!(taken[i].as_sparse().indices().len(), 1);
        }

        let contiguous = as_contiguous(&taken).unwrap();
        assert_eq!(
            contiguous
                .as_sparse()
                .indices()
                .as_primitive()
                .typed_data::<u64>(),
            [0u64, 7, 7, 9] // relative offsets
        );
        assert_eq!(
            contiguous
                .as_sparse()
                .values()
                .as_primitive()
                .typed_data::<f64>(),
            sparse.values().as_primitive().typed_data()
        );
    }

    #[test]
    fn test_take_map() {
        let sparse = sparse_array();
        let indices = PrimitiveArray::from((0u64..100).collect_vec());
        let (positions, patch_indices) = take_map(&sparse, &indices).unwrap();
        assert_eq!(
            positions.typed_data::<u64>(),
            sparse.indices().as_primitive().typed_data()
        );
        assert_eq!(patch_indices.typed_data::<u64>(), [0u64, 1, 2, 3]);
    }
}
