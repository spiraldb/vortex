use vortex_error::{VortexError, VortexResult};

use crate::array::Array;
use crate::compute::flatten::flatten;
use crate::compute::ArrayCompute;
use crate::scalar::Scalar;
use log::info;
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
    if let Some(search_sorted) = array.search_sorted() {
        return search_sorted.search_sorted(&scalar, side);
    }

    // Otherwise, flatten and try again.
    info!("SearchSorted not implemented for {}, flattening", array);
    flatten(array)?
        .into_array()
        .search_sorted()
        .map(|f| f.search_sorted(&scalar, side))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "search_sorted",
                array.encoding().id().name(),
            ))
        })
}

pub trait SearchSorted<T> {
    fn search_sorted(&self, value: &T, side: SearchSortedSide) -> usize;
}

impl<T: PartialOrd> SearchSorted<T> for &[T] {
    fn search_sorted(&self, value: &T, side: SearchSortedSide) -> usize {
        match side {
            SearchSortedSide::Left => self
                .binary_search_by(|x| {
                    if x < value {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                })
                .unwrap_or_else(|x| x),
            SearchSortedSide::Right => self
                .binary_search_by(|x| {
                    if x <= value {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                })
                .unwrap_or_else(|x| x),
        }
    }
}
