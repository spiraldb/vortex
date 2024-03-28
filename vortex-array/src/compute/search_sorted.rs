use std::cmp::Ordering;
use std::cmp::Ordering::{Equal, Greater, Less};

use vortex_error::{vortex_err, VortexResult};

use crate::array::Array;
use crate::compute::scalar_at::scalar_at;
use crate::scalar::Scalar;

#[derive(Debug, Copy, Clone)]
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

    if array.scalar_at().is_some() {
        return Ok(SearchSorted::search_sorted(&array, &scalar, side));
    }

    Err(vortex_err!(
        ni = "search_sorted",
        array.encoding().id().name()
    ))
}

pub trait IndexOrd<V> {
    fn index_cmp(&self, idx: usize, elem: &V) -> Option<Ordering>;

    fn index_lt(&self, idx: usize, elem: &V) -> bool {
        matches!(self.index_cmp(idx, elem), Some(Less))
    }

    fn index_le(&self, idx: usize, elem: &V) -> bool {
        matches!(self.index_cmp(idx, elem), Some(Less | Equal))
    }

    fn index_gt(&self, idx: usize, elem: &V) -> bool {
        matches!(self.index_cmp(idx, elem), Some(Greater))
    }

    fn index_ge(&self, idx: usize, elem: &V) -> bool {
        matches!(self.index_cmp(idx, elem), Some(Greater | Equal))
    }
}

#[allow(clippy::len_without_is_empty)]
pub trait Len {
    fn len(&self) -> usize;
}

pub trait SearchSorted<T> {
    fn search_sorted(&self, value: &T, side: SearchSortedSide) -> usize
    where
        Self: IndexOrd<T>,
    {
        match side {
            SearchSortedSide::Left => self.search_sorted_by(|idx| {
                if self.index_lt(idx, value) {
                    Less
                } else {
                    Greater
                }
            }),
            SearchSortedSide::Right => self.search_sorted_by(|idx| {
                if self.index_le(idx, value) {
                    Less
                } else {
                    Greater
                }
            }),
        }
    }

    fn search_sorted_by<F: FnMut(usize) -> Ordering>(&self, f: F) -> usize;
}

impl<S: IndexOrd<T> + Len + ?Sized, T> SearchSorted<T> for S {
    // Code adapted from Rust standard library slice::binary_search_by
    fn search_sorted_by<F: FnMut(usize) -> Ordering>(&self, mut f: F) -> usize {
        // INVARIANTS:
        // - 0 <= left <= left + size = right <= self.len()
        // - f returns Less for everything in self[..left]
        // - f returns Greater for everything in self[right..]
        let mut size = self.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;
            let cmp = f(mid);

            left = if cmp == Less { mid + 1 } else { left };
            right = if cmp == Greater { mid } else { right };
            if cmp == Equal {
                return mid;
            }

            size = right - left;
        }

        left
    }
}

impl IndexOrd<Scalar> for &dyn Array {
    fn index_cmp(&self, idx: usize, elem: &Scalar) -> Option<Ordering> {
        let scalar_a = scalar_at(*self, idx).ok()?;
        scalar_a.partial_cmp(elem)
    }
}

impl<T: PartialOrd> IndexOrd<T> for [T] {
    fn index_cmp(&self, idx: usize, elem: &T) -> Option<Ordering> {
        // SAFETY: Used in search_sorted_by same as the standard library. The search_sorted ensures idx is in bounds
        unsafe { self.get_unchecked(idx) }.partial_cmp(elem)
    }
}

impl Len for &dyn Array {
    fn len(&self) -> usize {
        Array::len(*self)
    }
}

impl<T> Len for [T] {
    fn len(&self) -> usize {
        self.len()
    }
}
