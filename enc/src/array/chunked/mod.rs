use std::vec::IntoIter;

use arrow::array::ArrayRef;
use itertools::Itertools;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone)]
pub struct ChunkedArray {
    chunks: Vec<Array>,
    chunk_ends: Vec<usize>,
    dtype: DType,
}

impl ChunkedArray {
    #[inline]
    pub fn new(chunks: Vec<Array>, dtype: DType) -> Self {
        assert!(
            chunks.iter().map(|chunk| chunk.dtype()).all_equal(),
            "Chunks have differing dtypes"
        );
        let mut chunk_ends = Vec::<usize>::with_capacity(chunks.len());
        for chunk in chunks.iter() {
            chunk_ends.push(chunk_ends.last().unwrap_or(&0usize) + chunk.len());
        }

        Self {
            chunks,
            chunk_ends,
            dtype,
        }
    }

    #[inline]
    pub fn chunks(&self) -> &[Array] {
        &self.chunks
    }

    fn find_physical_location(&self, index: usize) -> (usize, usize) {
        assert!(index <= self.len(), "Index out of bounds of the array");
        let index_chunk = self
            .chunk_ends
            .binary_search(&index)
            // If the result of binary_search is Ok it means we have exact match, since these are chunk ends EXCLUSIVE we have to add one to move to the next one
            .map(|o| o + 1)
            .unwrap_or_else(|o| o);
        let index_in_chunk = index
            - if index_chunk == 0 {
                0
            } else {
                self.chunk_ends[index_chunk - 1]
            };
        (index_chunk, index_in_chunk)
    }
}

impl ArrayEncoding for ChunkedArray {
    fn len(&self) -> usize {
        *self.chunk_ends.last().unwrap_or(&0usize)
    }

    fn is_empty(&self) -> bool {
        self.chunks.is_empty() || self.len() == 0
    }

    #[inline]
    fn dtype(&self) -> DType {
        self.dtype.clone()
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        let (chunk_index, chunk_offset) = self.find_physical_location(index);
        self.chunks[chunk_index].scalar_at(chunk_offset)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(ChunkedArrowIterator::new(self))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<Array> {
        self.check_slice_bounds(start, stop)?;

        let (offset_chunk, offset_in_first_chunk) = self.find_physical_location(start);
        let (length_chunk, length_in_last_chunk) = self.find_physical_location(stop);

        if length_chunk == offset_chunk {
            if let Some(chunk) = self.chunks.get(offset_chunk) {
                return Ok(Array::Chunked(ChunkedArray::new(
                    vec![chunk.slice(offset_in_first_chunk, length_in_last_chunk)?],
                    self.dtype.clone(),
                )));
            }
        }

        let mut chunks = self.chunks.clone()[offset_chunk..length_chunk + 1].to_vec();
        if let Some(c) = chunks.first_mut() {
            *c = c.slice(offset_in_first_chunk, c.len())?;
        }

        if length_in_last_chunk == 0 {
            chunks.pop();
        } else if let Some(c) = chunks.last_mut() {
            *c = c.slice(0, length_in_last_chunk)?;
        }

        Ok(Array::Chunked(ChunkedArray::new(
            chunks,
            self.dtype.clone(),
        )))
    }
}

struct ChunkedArrowIterator {
    chunks_iter: IntoIter<Array>,
    arrow_iter: Option<Box<ArrowIterator>>,
}

impl ChunkedArrowIterator {
    fn new(array: &ChunkedArray) -> Self {
        let mut chunks_iter = array.chunks.clone().into_iter();
        let arrow_iter = chunks_iter.next().map(|c| c.iter_arrow());
        Self {
            chunks_iter,
            arrow_iter,
        }
    }
}

impl Iterator for ChunkedArrowIterator {
    type Item = ArrayRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.arrow_iter
            .as_mut()
            .and_then(|iter| iter.next())
            .or_else(|| {
                self.chunks_iter.next().and_then(|next_chunk| {
                    self.arrow_iter = Some(next_chunk.iter_arrow());
                    self.next()
                })
            })
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use arrow::array::cast::AsArray;
    use arrow::array::types::UInt64Type;
    use arrow::array::ArrayRef;
    use arrow::array::ArrowPrimitiveType;
    use itertools::Itertools;

    use crate::array::chunked::ChunkedArray;
    use crate::array::ArrayEncoding;
    use crate::types::{DType, IntWidth};

    fn chunked_array() -> ChunkedArray {
        ChunkedArray::new(
            vec![
                vec![1u64, 2, 3].into(),
                vec![4u64, 5, 6].into(),
                vec![7u64, 8, 9].into(),
            ],
            DType::UInt(IntWidth::_64),
        )
    }

    fn assert_equal_slices<T: ArrowPrimitiveType>(arr: ArrayRef, slice: &[T::Native]) {
        assert_eq!(arr.as_primitive::<T>().values().deref(), slice);
    }

    #[test]
    pub fn iter() {
        let chunked = ChunkedArray::new(
            vec![vec![1u64, 2, 3].into(), vec![4u64, 5, 6].into()],
            DType::UInt(IntWidth::_64),
        );

        chunked
            .iter_arrow()
            .zip_eq([[1u64, 2, 3], [4, 5, 6]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }

    #[test]
    pub fn slice_middle() {
        chunked_array()
            .slice(2, 5)
            .unwrap()
            .iter_arrow()
            .zip_eq([vec![3u64], vec![4, 5]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }

    #[test]
    pub fn slice_begin() {
        chunked_array()
            .slice(1, 3)
            .unwrap()
            .iter_arrow()
            .zip_eq([[2u64, 3]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }

    #[test]
    pub fn slice_aligned() {
        chunked_array()
            .slice(3, 6)
            .unwrap()
            .iter_arrow()
            .zip_eq([[4u64, 5, 6]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }

    #[test]
    pub fn slice_many_aligned() {
        chunked_array()
            .slice(0, 6)
            .unwrap()
            .iter_arrow()
            .zip_eq([[1u64, 2, 3], [4, 5, 6]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }

    #[test]
    pub fn slice_end() {
        chunked_array()
            .slice(7, 8)
            .unwrap()
            .iter_arrow()
            .zip_eq([[8u64]])
            .for_each(|(arr, slice)| assert_equal_slices::<UInt64Type>(arr, &slice));
    }
}
