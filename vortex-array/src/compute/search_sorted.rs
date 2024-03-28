use vortex_error::{VortexError, VortexResult};

use crate::array::Array;
use crate::scalar::Scalar;
use std::cmp::Ordering;

pub enum SearchSortedSide {
    Left,
    Right,
}

pub trait SearchSortedFn {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<usize>;
}

pub fn search_sorted<T: Into<Scalar>>(
    array: &dyn Array,
    target: T,
    side: SearchSortedSide,
) -> VortexResult<usize> {
    let scalar = target.into().cast(array.dtype())?;
    array
        .search_sorted()
        .map(|f| f.search_sorted(&scalar, side))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "search_sorted",
                array.encoding().id().name(),
            ))
        })
}

pub trait SearchSortedManyFn {
    fn search_sorted_many(
        &self,
        values: &dyn Array,
        side: SearchSortedSide,
    ) -> VortexResult<Vec<usize>>;
}

pub fn search_sorted_many(
    array: &dyn Array,
    values: &dyn Array,
    side: SearchSortedSide,
) -> VortexResult<Vec<usize>> {
    array
        .search_sorted_many()
        .map(|f| f.search_sorted_many(values, side))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "search_sorted_many",
                array.encoding().id().name(),
            ))
        })
}

pub trait SearchSorted<T> {
    fn search_sorted(&self, value: &T, side: SearchSortedSide) -> usize {
        match side {
            SearchSortedSide::Left => self.search_sorted_left(value),
            SearchSortedSide::Right => self.search_sorted_right(value),
        }
    }

    fn search_sorted_left(&self, value: &T) -> usize;
    fn search_sorted_right(&self, value: &T) -> usize;
}

impl<T: PartialOrd> SearchSorted<T> for &[T] {
    fn search_sorted_left(&self, value: &T) -> usize {
        self.binary_search_by(|x| {
            if x < value {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
        .unwrap_or_else(|x| x)
    }

    fn search_sorted_right(&self, value: &T) -> usize {
        self.binary_search_by(|x| {
            if x <= value {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
        .unwrap_or_else(|x| x)
    }
}
