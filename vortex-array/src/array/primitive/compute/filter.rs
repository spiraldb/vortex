use vortex_dtype::{match_each_native_ptype, NativePType};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::primitive::PrimitiveArray;
use crate::compute::FilterFn;
use crate::stats::ArrayStatistics;
use crate::validity::filter_validity;
use crate::variants::BoolArrayTrait;
use crate::{Array, IntoArray};

impl FilterFn for PrimitiveArray {
    fn filter(&self, predicate: &Array) -> VortexResult<Array> {
        filter_select_primitive(self, predicate).map(|a| a.into_array())
    }
}

fn filter_select_primitive(
    arr: &PrimitiveArray,
    predicate: &Array,
) -> VortexResult<PrimitiveArray> {
    let Some(selection_count) = predicate.statistics().compute_true_count() else {
        vortex_bail!(
            NotImplemented: "compute_true_count",
            predicate.encoding().id()
        )
    };
    predicate.with_dyn(|b| {
        let validity = filter_validity(arr.validity(), predicate)?;
        if let Some(bb) = b.as_bool_array() {
            match_each_native_ptype!(arr.ptype(), |$T| {
                let slice = arr.maybe_null_slice::<$T>();
                Ok(PrimitiveArray::from_vec(filter_primitive_slice(slice, bb, selection_count), validity))
            })
        } else {
            vortex_bail!(
                NotImplemented: "as_bool_array",
                predicate.encoding().id()
            )
        }
    })
}

pub fn filter_primitive_slice<T: NativePType>(
    arr: &[T],
    predicate: &dyn BoolArrayTrait,
    selection_count: usize,
) -> Vec<T> {
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

        let filtered = filter_select_primitive(&arr, &bfilter.array()).unwrap();
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
