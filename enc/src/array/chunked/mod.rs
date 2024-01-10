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
        let offset_index = index;
        let index_chunk = self
            .chunk_ends
            .binary_search(&offset_index)
            // If the result of binary_search is Ok it means we have exact match, since these are chunk ends EXCLUSIVE we have to add one to move to the next one
            .map(|o| o + 1)
            .unwrap_or_else(|o| o);
        let index_in_chunk = offset_index
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
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        let (chunk_index, chunk_offset) = self.find_physical_location(index);
        self.chunks[chunk_index].scalar_at(chunk_offset)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator<'_>> {
        Box::new(ChunkedArrowIterator::new(self))
    }

    fn slice(&self, offset: usize, length: usize) -> Array {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Array {
        let (offset_chunk, offset_in_first_chunk) = self.find_physical_location(offset);
        let (length_chunk, length_in_last_chunk) = self.find_physical_location(offset + length);
        let offset_in_first_chunk = if offset_in_first_chunk == 0 {
            None
        } else {
            Some(offset_in_first_chunk)
        };
        let length_in_last_chunk = if length_in_last_chunk == self.chunks.last().unwrap().len() {
            None
        } else {
            Some(length_in_last_chunk)
        };

        if length_chunk == offset_chunk {
            if let Some(chunk) = self.chunks.get(offset_chunk) {
                let sliced_chunk = if let Some(off) = offset_in_first_chunk {
                    chunk.slice(off, length)
                } else {
                    chunk.to_owned()
                };
                return Array::Chunked(ChunkedArray::new(vec![sliced_chunk], self.dtype.clone()));
            }
        }

        let chunks = self
            .chunks
            .iter()
            .zip(0..length_chunk)
            .skip(offset_chunk)
            .map(|(chunk, idx)| {
                if idx == offset_chunk {
                    offset_in_first_chunk
                        .map(|off| chunk.slice(off, chunk.len() - off))
                        .unwrap_or(chunk.to_owned())
                } else if idx == length_chunk {
                    length_in_last_chunk
                        .map(|len| chunk.slice(0, len))
                        .unwrap_or(chunk.to_owned())
                } else {
                    chunk.to_owned()
                }
            })
            .collect::<Vec<Array>>();
        Array::Chunked(ChunkedArray::new(chunks, self.dtype.clone()))
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

    use crate::array::chunked::ChunkedArray;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::ArrayEncoding;
    use crate::types::{DType, IntWidth};

    fn chunked_array() -> ChunkedArray {
        let arrow_chunk1 = ArrowPrimitiveArray::<u64>::from_vec(vec![1, 2, 3]);
        let arrow_chunk2 = ArrowPrimitiveArray::<u64>::from_vec(vec![4, 5, 6]);
        let arrow_chunk3 = ArrowPrimitiveArray::<u64>::from_vec(vec![7, 8, 9]);
        let chunk1: PrimitiveArray = (&arrow_chunk1).into();
        let chunk2: PrimitiveArray = (&arrow_chunk2).into();
        let chunk3: PrimitiveArray = (&arrow_chunk3).into();
        ChunkedArray::new(
            vec![chunk1.into(), chunk2.into(), chunk3.into()],
            DType::UInt(IntWidth::_64),
        )
    }

    #[test]
    pub fn iter() {
        let arrow_chunk1 = ArrowPrimitiveArray::<u64>::from_vec(vec![1, 2, 3]);
        let arrow_chunk2 = ArrowPrimitiveArray::<u64>::from_vec(vec![4, 5, 6]);
        let chunk1: PrimitiveArray = (&arrow_chunk1).into();
        let chunk2: PrimitiveArray = (&arrow_chunk2).into();
        let chunked = ChunkedArray::new(
            vec![chunk1.into(), chunk2.into()],
            DType::UInt(IntWidth::_64),
        );

        chunked
            .iter_arrow()
            .zip(vec![arrow_chunk1, arrow_chunk2].iter())
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap(),
                    orig
                );
            });
    }

    #[test]
    pub fn slice_middle() {
        chunked_array()
            .slice(2, 3)
            .iter_arrow()
            .zip(
                [
                    ArrowPrimitiveArray::<u64>::from_vec(vec![3]),
                    ArrowPrimitiveArray::<u64>::from_vec(vec![4, 5]),
                ]
                .iter(),
            )
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap(),
                    orig
                );
            });
    }
    #[test]
    pub fn slice_begin() {
        chunked_array()
            .slice(1, 2)
            .iter_arrow()
            .zip([ArrowPrimitiveArray::<u64>::from_vec(vec![2, 3])].iter())
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap(),
                    orig
                );
            });
    }

    #[test]
    pub fn slice_aligned() {
        chunked_array()
            .slice(3, 3)
            .iter_arrow()
            .zip([ArrowPrimitiveArray::<u64>::from_vec(vec![4, 5, 6])].iter())
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap(),
                    orig
                );
            });
    }
    #[test]
    pub fn slice_end() {
        chunked_array()
            .slice(7, 1)
            .iter_arrow()
            .zip([ArrowPrimitiveArray::<u64>::from_vec(vec![8])].iter())
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<u64>>()
                        .unwrap(),
                    orig
                );
            });
    }
}
