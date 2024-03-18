use crate::array::Array;
use arrow_buffer::{BooleanBuffer, NullBuffer};

pub trait ArrayAccessor: Array {
    /// The type of the element being accessed.
    type Item: Send + Sync;

    /// Returns the element at index.
    /// Panics if the value is outside the bounds of the array
    fn value(&self, index: usize) -> Self::Item;

    /// Returns the element at index.
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    unsafe fn value_unchecked(&self, index: usize) -> Self::Item;
}

#[derive(Debug)]
pub struct ArrayIter<T: ArrayAccessor> {
    array: T,
    logical_nulls: Option<BooleanBuffer>,
    current: usize,
    current_end: usize,
}

impl<T: ArrayAccessor> ArrayIter<T> {
    /// create a new iterator
    pub fn new(array: T) -> Self {
        let len = array.len();
        let logical_nulls = array.l();
        ArrayIter {
            array,
            logical_nulls,
            current: 0,
            current_end: len,
        }
    }

    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        self.logical_nulls
            .as_ref()
            .map(|x| x.is_null(idx))
            .unwrap_or_default()
    }
}

impl<T: ArrayAccessor> Iterator for arrow_array::iterator::ArrayIter<T> {
    type Item = Option<T::Item>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else if self.is_null(self.current) {
            self.current += 1;
            Some(None)
        } else {
            let old = self.current;
            self.current += 1;
            // Safety:
            // we just checked bounds in `self.current_end == self.current`
            // this is safe on the premise that this struct is initialized with
            // current = array.len()
            // and that current_end is ever only decremented
            unsafe { Some(Some(self.array.value_unchecked(old))) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}
