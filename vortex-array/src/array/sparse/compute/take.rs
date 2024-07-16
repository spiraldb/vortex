use std::collections::HashMap;

use itertools::Itertools;
use vortex_dtype::match_each_integer_ptype;
use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::SparseArray;
use crate::compute::{take, TakeFn};
use crate::{Array, IntoArray, IntoArrayVariant};

impl TakeFn for SparseArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let flat_indices = indices.clone().into_primitive()?;
        // if we are taking a lot of values we should build a hashmap
        let (positions, physical_take_indices) = if indices.len() > 128 {
            take_map(self, &flat_indices)?
        } else {
            take_search_sorted(self, &flat_indices)?
        };

        let taken_values = take(&self.values(), &physical_take_indices.into_array())?;

        Ok(Self::try_new(
            positions.into_array(),
            taken_values,
            indices.len(),
            self.fill_value().clone(),
        )
        .expect("Must be valid SparseArray")
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
        indices.maybe_null_slice::<$P>()
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
            .maybe_null_slice::<$P>()
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
    use crate::array::sparse::compute::take::take_map;
    use crate::array::sparse::SparseArray;
    use crate::compute::take;
    use crate::validity::Validity;
    use crate::{Array, IntoArray, IntoArrayVariant};

    fn sparse_array() -> Array {
        SparseArray::try_new(
            PrimitiveArray::from(vec![0u64, 37, 47, 99]).into_array(),
            PrimitiveArray::from_vec(vec![1.23f64, 0.47, 9.99, 3.5], Validity::AllValid)
                .into_array(),
            100,
            Scalar::null(DType::Primitive(PType::F64, Nullability::Nullable)),
        )
        .unwrap()
        .into_array()
    }

    #[test]
    fn sparse_take() {
        let sparse = sparse_array();
        let taken =
            SparseArray::try_from(take(&sparse, &vec![0, 47, 47, 0, 99].into_array()).unwrap())
                .unwrap();
        assert_eq!(
            taken
                .indices()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<u64>(),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(
            taken
                .values()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<f64>(),
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
            .unwrap()
            .maybe_null_slice::<u64>()
            .is_empty());
        assert!(taken
            .values()
            .into_primitive()
            .unwrap()
            .maybe_null_slice::<f64>()
            .is_empty());
    }

    #[test]
    fn ordered_take() {
        let sparse = sparse_array();
        let taken =
            SparseArray::try_from(take(&sparse, &vec![69, 37].into_array()).unwrap()).unwrap();
        assert_eq!(
            taken
                .indices()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<u64>(),
            [1]
        );
        assert_eq!(
            taken
                .values()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<f64>(),
            [0.47f64]
        );
        assert_eq!(taken.len(), 2);
    }

    #[test]
    fn test_take_map() {
        let sparse = SparseArray::try_from(sparse_array()).unwrap();
        let indices = PrimitiveArray::from((0u64..100).collect_vec());
        let (positions, patch_indices) = take_map(&sparse, &indices).unwrap();
        assert_eq!(
            positions.maybe_null_slice::<u64>(),
            sparse
                .indices()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<u64>()
        );
        assert_eq!(patch_indices.maybe_null_slice::<u64>(), [0u64, 1, 2, 3]);
    }
}
