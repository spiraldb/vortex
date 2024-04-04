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
use crate::{compute, match_each_integer_ptype, match_each_native_ptype};

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
        .into_array())
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
        let indices = compute::flatten::flatten_primitive(indices)?;
        let adjusted_indices = match_each_integer_ptype!(indices.ptype(), |$P| {
             indices.typed_data::<$P>()
                .iter()
                .map(|i| *i as usize + self.indices_offset)
                .collect::<Vec<_>>()
        });

        // TODO(robert): Use binary search instead of search_sorted + take and index validation to avoid extra work
        let physical_indices = PrimitiveArray::from(
            adjusted_indices
                .iter()
                .map(|i| {
                    search_sorted(self.indices(), *i, SearchSortedSide::Left).map(|s| s as u64)
                })
                .collect::<VortexResult<Vec<_>>>()?,
        );
        let taken_indices =
            compute::flatten::flatten_primitive(&take(self.indices(), &physical_indices)?)?;
        let exact_taken_indices = match_each_integer_ptype!(taken_indices.ptype(), |$P| {
                PrimitiveArray::from(taken_indices
                    .typed_data::<$P>()
                    .iter()
                    .copied()
                    .zip_eq(adjusted_indices)
                    .zip_eq(physical_indices.typed_data::<u64>())
                    .filter(|((taken_idx, orig_idx), _)| *taken_idx as usize == *orig_idx)
                    .map(|(_, physical_idx)| *physical_idx)
                    .collect::<Vec<_>>())
        });

        let taken_values = take(self.values(), &exact_taken_indices)?;

        Ok(SparseArray::new(
            PrimitiveArray::from((0u64..exact_taken_indices.len() as u64).collect::<Vec<_>>())
                .into_array(),
            taken_values,
            indices.len(),
            self.fill_value().clone(),
        )
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex_schema::{DType, FloatWidth, Nullability};

    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::sparse::SparseArray;
    use crate::array::Array;
    use crate::compute::take::take;
    use crate::scalar::Scalar;

    #[test]
    fn sparse_take() {
        let sparse = SparseArray::new(
            PrimitiveArray::from(vec![0u64, 37, 47, 99]).into_array(),
            PrimitiveArray::from(vec![1.23f64, 0.47, 9.99, 3.5]).into_array(),
            100,
            Scalar::null(&DType::Float(FloatWidth::_64, Nullability::Nullable)),
        );
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
        let sparse = SparseArray::new(
            PrimitiveArray::from(vec![0u64, 37, 47, 99]).into_array(),
            PrimitiveArray::from(vec![1.23f64, 0.47, 9.99, 3.5]).into_array(),
            100,
            Scalar::null(&DType::Float(FloatWidth::_64, Nullability::Nullable)),
        );
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
}
