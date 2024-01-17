use arrow::array::{Array, Datum};
use polars_core::prelude::Series;
use polars_ops::prelude::search_sorted;

use crate::arrow::polars::{IntoPolarsSeries, IntoPolarsValue};
use crate::error::EncResult;

pub enum SearchSortedSide {
    Left,
    Right,
}

pub fn search_sorted_scalar(
    haystack: Vec<&dyn Array>,
    needle: &dyn Datum,
    side: SearchSortedSide,
) -> EncResult<usize> {
    let haystack_series: Series = haystack.into_polars();
    let needle_series = Series::from_any_values("needle", &[needle.into_polars()], true)?;
    Ok(
        search_sorted(&haystack_series, &needle_series, side.into(), false)?
            .get(0)
            .unwrap() as usize,
    )
}

impl From<SearchSortedSide> for polars_ops::prelude::SearchSortedSide {
    fn from(value: SearchSortedSide) -> Self {
        match value {
            SearchSortedSide::Left => polars_ops::prelude::SearchSortedSide::Left,
            SearchSortedSide::Right => polars_ops::prelude::SearchSortedSide::Right,
        }
    }
}

#[cfg(test)]
mod test {
    use arrow::array::{Int32Array, Scalar};

    use super::*;

    #[test]
    fn test_searchsorted_scalar() {
        let haystack = Int32Array::from(vec![Some(1), Some(2), Some(3)]);

        assert_eq!(
            search_sorted_scalar(
                vec![&haystack],
                &Scalar::new(Int32Array::from(vec![0])),
                SearchSortedSide::Left,
            )
            .unwrap(),
            0
        );
        assert_eq!(
            search_sorted_scalar(
                vec![&haystack],
                &Scalar::new(Int32Array::from(vec![1])),
                SearchSortedSide::Left,
            )
            .unwrap(),
            0
        );
        assert_eq!(
            search_sorted_scalar(
                vec![&haystack],
                &Scalar::new(Int32Array::from(vec![1])),
                SearchSortedSide::Right,
            )
            .unwrap(),
            1
        );
        assert_eq!(
            search_sorted_scalar(
                vec![&haystack],
                &Scalar::new(Int32Array::from(vec![4])),
                SearchSortedSide::Left,
            )
            .unwrap(),
            3
        );
    }
}
