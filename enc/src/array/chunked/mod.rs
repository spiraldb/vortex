use arrow2::array::Array as ArrowArray;
use itertools::Itertools;
use std::slice::Iter;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
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

    fn iter_arrow(&self) -> Box<ArrowIterator<'_>> {
        Box::new(ChunkedArrowIterator::new(self))
    }

    fn slice(&self, offset: usize, length: usize) -> EncResult<Array> {
        // TODO(ngates): make assertions raise error
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        let (offset_chunk, offset_in_first_chunk) = self.find_physical_location(offset);
        let (length_chunk, length_in_last_chunk) = self.find_physical_location(offset + length);

        if length_chunk == offset_chunk {
            if let Some(chunk) = self.chunks.get(offset_chunk) {
                return Ok(Array::Chunked(ChunkedArray::new(
                    vec![chunk.slice(offset_in_first_chunk, length)?],
                    self.dtype.clone(),
                )));
            }
        }

        let mut chunks = self.chunks.clone()[offset_chunk..length_chunk + 1].to_vec();
        if let Some(c) = chunks.first_mut() {
            *c = c.slice(offset_in_first_chunk, c.len() - offset_in_first_chunk)?;
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

struct ChunkedArrowIterator<'a> {
    chunks_iter: Iter<'a, Array>,
    arrow_iter: Option<Box<ArrowIterator<'a>>>,
}

impl<'a> ChunkedArrowIterator<'a> {
    fn new(array: &'a ChunkedArray) -> Self {
        let mut chunks_iter = array.chunks.iter();
        let arrow_iter = chunks_iter.next().map(|c| c.iter_arrow());
        Self {
            chunks_iter,
            arrow_iter,
        }
    }
}

impl<'a> Iterator for ChunkedArrowIterator<'a> {
    type Item = Box<dyn ArrowArray>;

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
    use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;
    use itertools::Itertools;

    use crate::array::chunked::ChunkedArray;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::ArrayEncoding;
    use crate::types::{DType, IntWidth};

    fn chunked_array() -> ChunkedArray {
        let chunk1: PrimitiveArray = ArrowPrimitiveArray::<u64>::from_vec(vec![1, 2, 3]).into();
        let chunk2: PrimitiveArray = ArrowPrimitiveArray::<u64>::from_vec(vec![4, 5, 6]).into();
        let chunk3: PrimitiveArray = ArrowPrimitiveArray::<u64>::from_vec(vec![7, 8, 9]).into();
        ChunkedArray::new(
            vec![chunk1.into(), chunk2.into(), chunk3.into()],
            DType::UInt(IntWidth::_64),
        )
    }

    #[test]
    pub fn iter() {
        let chunk1: PrimitiveArray = ArrowPrimitiveArray::<u64>::from_vec(vec![1, 2, 3]).into();
        let chunk2: PrimitiveArray = ArrowPrimitiveArray::<u64>::from_vec(vec![4, 5, 6]).into();
        let chunked = ChunkedArray::new(
            vec![chunk1.into(), chunk2.into()],
            DType::UInt(IntWidth::_64),
        );

        chunked
            .iter_arrow()
            .zip_eq([[1u64, 2, 3], [4, 5, 6]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap()
                        .values()
                        .as_slice(),
                    orig
                );
            });
    }

    #[test]
    pub fn slice_middle() {
        chunked_array()
            .slice(2, 3)
            .unwrap()
            .iter_arrow()
            .zip_eq([vec![3], vec![4, 5]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap()
                        .values()
                        .as_slice(),
                    orig
                );
            });
    }

    #[test]
    pub fn slice_begin() {
        chunked_array()
            .slice(1, 2)
            .unwrap()
            .iter_arrow()
            .zip_eq([[2, 3]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap()
                        .values()
                        .as_slice(),
                    orig
                );
            });
    }

    #[test]
    pub fn slice_aligned() {
        chunked_array()
            .slice(3, 3)
            .unwrap()
            .iter_arrow()
            .zip_eq([[4, 5, 6]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap()
                        .values()
                        .as_slice(),
                    orig
                );
            });
    }

    #[test]
    pub fn slice_many_aligned() {
        chunked_array()
            .slice(0, 6)
            .unwrap()
            .iter_arrow()
            .zip_eq([[1, 2, 3], [4, 5, 6]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap()
                        .values()
                        .as_slice(),
                    orig
                );
            });
    }

    #[test]
    pub fn slice_end() {
        chunked_array()
            .slice(7, 1)
            .unwrap()
            .iter_arrow()
            .zip_eq([[8]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap()
                        .values()
                        .as_slice(),
                    orig
                );
            });
    }
}
