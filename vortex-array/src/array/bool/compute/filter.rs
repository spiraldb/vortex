use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};

use crate::array::bool::BoolArray;
use crate::compute::FilterFn;
use crate::stats::ArrayStatistics;
use crate::validity::filter_validity;
use crate::variants::BoolArrayTrait;
use crate::{Array, IntoArray, ToArray};

impl FilterFn for BoolArray {
    fn filter(&self, predicate: &Array) -> Array {
        let bool_array = BoolArray::try_from(predicate).unwrap();
        filter_select_bool(self, &bool_array).into_array()
    }
}

fn filter_select_bool(arr: &BoolArray, predicate: &BoolArray) -> BoolArray {
    let selection_count = predicate.statistics().compute_true_count().unwrap();
    let out = if selection_count * 2 > predicate.len() {
        filter_select_bool_by_slice(&arr.boolean_buffer(), predicate, selection_count)
    } else {
        filter_select_bool_by_index(&arr.boolean_buffer(), predicate, selection_count)
    };
    BoolArray::try_new(out, filter_validity(arr.validity(), &predicate.to_array())).unwrap()
}

fn filter_select_bool_by_slice(
    values: &BooleanBuffer,
    predicate: &BoolArray,
    selection_count: usize,
) -> BooleanBuffer {
    let mut out_buf = BooleanBufferBuilder::new(selection_count);
    predicate.maybe_null_slices_iter().for_each(|(start, end)| {
        out_buf.append_buffer(&values.slice(start, end - start));
    });
    out_buf.finish()
}

fn filter_select_bool_by_index(
    values: &BooleanBuffer,
    predicate: &BoolArray,
    selection_count: usize,
) -> BooleanBuffer {
    let mut out_buf = BooleanBufferBuilder::new(selection_count);
    predicate
        .maybe_null_indices_iter()
        .for_each(|idx| out_buf.append(values.value(idx)));
    out_buf.finish()
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use crate::array::bool::compute::filter::{
        filter_select_bool, filter_select_bool_by_index, filter_select_bool_by_slice,
    };
    use crate::array::bool::BoolArray;

    #[test]
    fn filter_bool_test() {
        let arr = BoolArray::from(vec![true, true, false]);
        let filter = BoolArray::from(vec![true, false, true]);

        let filtered = filter_select_bool(&arr, &filter);
        assert_eq!(2, filtered.len());

        assert_eq!(
            vec![true, false],
            filtered.boolean_buffer().iter().collect_vec()
        )
    }

    #[test]
    fn filter_bool_by_slice_test() {
        let arr = BoolArray::from(vec![true, true, false]);
        let filter = BoolArray::from(vec![true, false, true]);

        let filtered = filter_select_bool_by_slice(&arr.boolean_buffer(), &filter, 2);
        assert_eq!(2, filtered.len());

        assert_eq!(vec![true, false], filtered.iter().collect_vec())
    }

    #[test]
    fn filter_bool_by_index_test() {
        let arr = BoolArray::from(vec![true, true, false]);
        let filter = BoolArray::from(vec![true, false, true]);

        let filtered = filter_select_bool_by_index(&arr.boolean_buffer(), &filter, 2);
        assert_eq!(2, filtered.len());

        assert_eq!(vec![true, false], filtered.iter().collect_vec())
    }
}
