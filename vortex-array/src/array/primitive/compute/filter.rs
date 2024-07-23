use vortex_dtype::{match_each_native_ptype, NativePType};

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::compute::FilterFn;
use crate::stats::ArrayStatistics;
use crate::validity::filter_validity;
use crate::variants::BoolArrayTrait;
use crate::{Array, IntoArray};

impl FilterFn for PrimitiveArray {
    fn filter(&self, predicate: &Array) -> Array {
        let bool_array = BoolArray::try_from(predicate).unwrap();
        filter_select_primitive(self, &bool_array).into_array()
    }
}

fn filter_select_primitive(arr: &PrimitiveArray, bools: &BoolArray) -> PrimitiveArray {
    let selection_count = bools.statistics().compute_true_count().unwrap();
    let validity = filter_validity(arr.validity(), bools.array());
    match_each_native_ptype!(arr.ptype(), |$T| {
        let slice = arr.maybe_null_slice::<$T>();
        PrimitiveArray::from_vec(filter_primitive_slice(slice, bools, selection_count), validity)
    })
}

pub fn filter_primitive_slice<T: NativePType>(
    arr: &[T],
    predicate: &BoolArray,
    selection_count: usize,
) -> Vec<T> {
    let mut _start_pos = 0;
    let mut chunks = Vec::with_capacity(selection_count);
    if selection_count * 2 > predicate.len() {
        predicate.maybe_null_slices_iter().for_each(|(start, end)| {
            chunks.extend_from_slice(&arr[start..end]);
        });
    } else {
        chunks.extend(predicate.maybe_null_indices_iter().map(|idx| arr[idx]));
    }
    chunks
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use crate::array::bool::BoolArray;
    use crate::array::primitive::compute::filter::filter_select_primitive;
    use crate::array::primitive::PrimitiveArray;

    #[test]
    fn filter_run_variant_mixed_test() {
        let filter = vec![true, true, false, true, true, true, false, true];
        let bfilter = BoolArray::from(filter.clone());
        let arr = PrimitiveArray::from(vec![1u32, 24, 54, 2, 3, 2, 3, 2]);

        let filtered = filter_select_primitive(&arr, &bfilter);
        assert_eq!(
            filtered.len(),
            filter.iter().filter(|x| **x).collect_vec().len()
        );

        let rust_arr = arr.maybe_null_slice::<u32>();
        assert_eq!(
            filtered.maybe_null_slice::<u32>().to_vec(),
            filter
                .iter()
                .enumerate()
                .filter(|(_idx, b)| **b)
                .map(|m| rust_arr[m.0])
                .collect_vec()
        )
    }
}
