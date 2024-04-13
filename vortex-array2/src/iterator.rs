use std::marker::PhantomData;

use crate::accessor::ArrayAccessor;

pub struct ArrayIter<'a, A: ArrayAccessor<'a, T>, T> {
    array: &'a A,
    current: usize,
    end: usize,
    phantom: PhantomData<T>,
}

impl<'a, A: ArrayAccessor<'a, T>, T> ArrayIter<'a, A, T> {
    pub fn new(array: &'a A) -> Self {
        let len = array.len();
        ArrayIter {
            array,
            current: 0,
            end: len,
            phantom: PhantomData,
        }
    }
}

impl<'a, A: ArrayAccessor<'a, T>, T> Iterator for ArrayIter<'a, A, T> {
    type Item = Option<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            None
        } else {
            let old = self.current;
            self.current += 1;
            Some(self.array.value(old))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

impl<'a, A: ArrayAccessor<'a, T>, T> DoubleEndedIterator for ArrayIter<'a, A, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.end == self.current {
            None
        } else {
            self.end -= 1;
            Some(self.array.value(self.end))
        }
    }
}

impl<'a, A: ArrayAccessor<'a, T>, T> ExactSizeIterator for ArrayIter<'a, A, T> {}
