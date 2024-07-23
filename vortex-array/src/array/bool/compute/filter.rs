use arrow_array::BooleanArray;
use arrow_buffer::bit_iterator::BitIndexIterator;
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_select::filter::SlicesIterator;

use crate::array::bool::BoolArray;
use crate::compute::FilterFn;
use crate::stats::ArrayStatistics;
use crate::validity::filter_validity;
use crate::{Array, IntoArray, ToArray};

impl FilterFn for BoolArray {
    fn filter(&self, predicate: &Array) -> Array {
        let bool_array = BoolArray::try_from(predicate).unwrap();
        filter_select_bool(self, &bool_array).into_array()
    }
}

fn filter_select_bool(arr: &BoolArray, bools: &BoolArray) -> BoolArray {
    let selection_count = bools.statistics().compute_true_count().unwrap();
    let out = if selection_count * 2 > bools.len() {
        filter_select_bool_by_slice(
            &arr.boolean_buffer(),
            &bools.boolean_buffer(),
            selection_count,
        )
    } else {
        filter_select_bool_by_index(
            &arr.boolean_buffer(),
            &bools.boolean_buffer(),
            selection_count,
        )
    };
    BoolArray::try_new(out, filter_validity(arr.validity(), &bools.to_array())).unwrap()
}

fn filter_select_bool_by_slice(
    values: &BooleanBuffer,
    predicate: &BooleanBuffer,
    selection_count: usize,
) -> BooleanBuffer {
    let mut bool_buffer_out = BooleanBufferBuilder::new(selection_count);
    for (start, end) in SlicesIterator::new(&BooleanArray::new(predicate.clone(), None)) {
        let sl = &values.slice(start, end - start);
        bool_buffer_out.append_buffer(sl);
    }
    bool_buffer_out.finish()
}

fn filter_select_bool_by_index(
    values: &BooleanBuffer,
    predicate: &BooleanBuffer,
    selection_count: usize,
) -> BooleanBuffer {
    let mut bool_buffer_out = BooleanBufferBuilder::new(selection_count);

    for i in BitIndexIterator::new(predicate.values(), 0, predicate.len()) {
        bool_buffer_out.append(values.value(i));
    }
    bool_buffer_out.finish()
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

        let filtered =
            filter_select_bool_by_slice(&arr.boolean_buffer(), &filter.boolean_buffer(), 2);
        assert_eq!(2, filtered.len());

        assert_eq!(vec![true, false], filtered.iter().collect_vec())
    }

    #[test]
    fn filter_bool_by_index_test() {
        let arr = BoolArray::from(vec![true, true, false]);
        let filter = BoolArray::from(vec![true, false, true]);

        let filtered =
            filter_select_bool_by_index(&arr.boolean_buffer(), &filter.boolean_buffer(), 2);
        assert_eq!(2, filtered.len());

        assert_eq!(vec![true, false], filtered.iter().collect_vec())
    }
}
