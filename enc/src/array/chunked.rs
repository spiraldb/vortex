use std::slice::Iter;

use arrow2::array::Array as ArrowArray;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkedArray {
    chunks: Vec<Array>,
    dtype: DType,
}

impl ChunkedArray {
    #[inline]
    pub fn new(chunks: Vec<Array>, dtype: DType) -> Self {
        // TODO(ngates): assert all chunks have the correct DType.
        Self { chunks, dtype }
    }

    #[inline]
    pub fn chunks(&self) -> &[Array] {
        &self.chunks
    }
}

impl ArrayEncoding for ChunkedArray {
    fn len(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }

    fn is_empty(&self) -> bool {
        self.chunks.is_empty() || self.len() == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn scalar_at(&self, _index: usize) -> EncResult<Box<dyn Scalar>> {
        todo!()
    }

    fn iter_arrow(&self) -> Box<ArrowIterator<'_>> {
        Box::new(ChunkedArrowIterator::new(self))
    }

    fn slice(&self, _offset: usize, _length: usize) -> Array {
        todo!()
    }

    unsafe fn slice_unchecked(&self, _offset: usize, _length: usize) -> Array {
        todo!()
    }
}

struct ChunkedArrowIterator<'a> {
    // An arrow iterator over chunked arrays, with lifetime tied to the underlying array.
    chunks_iter: Iter<'a, Array>,
    arrow_iter: Option<Box<ArrowIterator<'a>>>,
}

impl<'a> ChunkedArrowIterator<'a> {
    fn new(array: &'a ChunkedArray) -> Self {
        let mut chunks_iter = array.chunks.iter();
        let arrow_iter = chunks_iter.next().map(|chunk| chunk.iter_arrow());
        Self {
            chunks_iter,
            arrow_iter,
        }
    }
}

impl<'a> Iterator for ChunkedArrowIterator<'a> {
    type Item = Box<dyn ArrowArray>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.arrow_iter.as_mut() {
            Some(iter) => match iter.next() {
                Some(item) => Some(item),
                None => {
                    self.arrow_iter = self.chunks_iter.next().map(|chunk| chunk.iter_arrow());
                    self.next()
                }
            },
            None => None,
        }
    }
}
