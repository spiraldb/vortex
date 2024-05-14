use std::collections::HashMap;

use itertools::Itertools;
use vortex_dtype::match_each_integer_ptype;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::SparseArray;
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::slice::SliceFn;
use crate::compute::take::{take, TakeFn};
use crate::compute::ArrayCompute;
use crate::{Array, ArrayDType, ArrayTrait, IntoArray};

mod slice;

impl ArrayCompute for SparseArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl AsContiguousFn for SparseArray {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<Array> {
        let sparse = arrays
            .iter()
            .map(|a| SparseArray::try_from(a).unwrap())
            .collect_vec();

        if !sparse.iter().map(|a| a.fill_value()).all_equal() {
            vortex_bail!("Cannot concatenate SparseArrays with differing fill values");
        }

        Ok(SparseArray::new(
            as_contiguous(&sparse.iter().map(|a| a.indices()).collect_vec())?,
            as_contiguous(&sparse.iter().map(|a| a.values()).collect_vec())?,
            sparse.iter().map(|a| a.len()).sum(),
            self.fill_value().clone(),
        )
        .into_array())
    }
}

impl ScalarAtFn for SparseArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match self.find_index(index)? {
            None => self.fill_value().clone().cast(self.dtype()),
            Some(idx) => scalar_at(&self.values(), idx)?.cast(self.dtype()),
        }
    }
}

impl TakeFn for SparseArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let flat_indices = indices.clone().flatten_primitive()?;
        // if we are taking a lot of values we should build a hashmap
        let (positions, physical_take_indices) = if indices.len() > 128 {
            take_map(self, &flat_indices)?
        } else {
            take_search_sorted(self, &flat_indices)?
        };

        let taken_values = take(&self.values(), &physical_take_indices.into_array())?;

        Ok(SparseArray::new(
            positions.into_array(),
            taken_values,
            indices.len(),
            self.fill_value().clone(),
        )
        .into_array())
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
    let resolved = match_each_integer_ptype!(indices.ptype(), |$P| {
        indices
            .typed_data::<$P>()
            .iter()
            .enumerate()
            .map(|(pos, i)| {
                array
                    .find_index(*i as usize)
                    .map(|r| r.map(|ii| (pos as u64, ii as u64)))
            })
            .filter_map_ok(|r| r)
            .collect::<VortexResult<Vec<_>>>()?
    });

    let (positions, patch_indices): (Vec<u64>, Vec<u64>) = resolved.into_iter().unzip();
    Ok((
        PrimitiveArray::from(positions),
        PrimitiveArray::from(patch_indices),
    ))
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use vortex_dtype::{DType, Nullability, PType};
    use vortex_scalar::Scalar;

    use crate::array::primitive::PrimitiveArray;
    use crate::array::sparse::compute::take_map;
    use crate::array::sparse::SparseArray;
    use crate::compute::as_contiguous::as_contiguous;
    use crate::compute::slice::slice;
    use crate::compute::take::take;
    use crate::validity::Validity;
    use crate::{Array, ArrayTrait, IntoArray};

    fn sparse_array() -> Array {
        SparseArray::new(
            PrimitiveArray::from(vec![0u64, 37, 47, 99]).into_array(),
            PrimitiveArray::from_vec(vec![1.23f64, 0.47, 9.99, 3.5], Validity::AllValid)
                .into_array(),
            100,
            Scalar::null(DType::Primitive(PType::F64, Nullability::Nullable)),
        )
        .into_array()
    }

    #[test]
    fn sparse_take() {
        let sparse = sparse_array();
        let taken =
            SparseArray::try_from(take(&sparse, &vec![0, 47, 47, 0, 99].into_array()).unwrap())
                .unwrap();
        assert_eq!(
            taken.indices().into_primitive().typed_data::<u64>(),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(
            taken.values().into_primitive().typed_data::<f64>(),
            [1.23f64, 9.99, 9.99, 1.23, 3.5]
        );
    }

    #[test]
    fn nonexistent_take() {
        let sparse = sparse_array();
        let taken = SparseArray::try_from(take(&sparse, &vec![69].into_array()).unwrap()).unwrap();
        assert!(taken
            .indices()
            .into_primitive()
            .typed_data::<u64>()
            .is_empty());
        assert!(taken
            .values()
            .into_primitive()
            .typed_data::<f64>()
            .is_empty());
    }

    #[test]
    fn ordered_take() {
        let sparse = sparse_array();
        let taken =
            SparseArray::try_from(take(&sparse, &vec![69, 37].into_array()).unwrap()).unwrap();
        assert_eq!(taken.indices().into_primitive().typed_data::<u64>(), [1]);
        assert_eq!(
            taken.values().into_primitive().typed_data::<f64>(),
            [0.47f64]
        );
        assert_eq!(taken.len(), 2);
    }

    #[test]
    fn take_slices_and_reassemble() {
        let sparse = sparse_array();
        let slices = (0..10)
            .map(|i| slice(&sparse, i * 10, (i + 1) * 10).unwrap())
            .collect_vec();

        let taken = slices
            .iter()
            .map(|s| take(s, &(0u64..10).collect_vec().into_array()).unwrap())
            .collect_vec();
        for i in [1, 2, 5, 6, 7, 8] {
            assert_eq!(SparseArray::try_from(&taken[i]).unwrap().indices().len(), 0);
        }
        for i in [0, 3, 4, 9] {
            assert_eq!(SparseArray::try_from(&taken[i]).unwrap().indices().len(), 1);
        }

        let contiguous = SparseArray::try_from(as_contiguous(&taken).unwrap()).unwrap();
        assert_eq!(
            contiguous.indices().into_primitive().typed_data::<u64>(),
            [0u64, 7, 7, 9] // relative offsets
        );
        assert_eq!(
            contiguous.values().into_primitive().typed_data::<f64>(),
            SparseArray::try_from(sparse)
                .unwrap()
                .values()
                .into_primitive()
                .typed_data::<f64>()
        );
    }

    #[test]
    fn test_take_map() {
        let sparse = SparseArray::try_from(sparse_array()).unwrap();
        let indices = PrimitiveArray::from((0u64..100).collect_vec());
        let (positions, patch_indices) = take_map(&sparse, &indices).unwrap();
        assert_eq!(
            positions.typed_data::<u64>(),
            sparse.indices().into_primitive().typed_data::<u64>()
        );
        assert_eq!(patch_indices.typed_data::<u64>(), [0u64, 1, 2, 3]);
    }
}
