use std::iter;

use arrow2::array::Array as ArrowArray;
use itertools::Itertools;

use crate::array::{Array, ArrayEncoding, ArrowIterator, IntoArrowIterator};
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkedArray {
    chunks: Vec<Array>,
    chunk_ends: Vec<usize>,
    dtype: DType,
    offset: usize,
    length: usize,
}

impl ChunkedArray {
    #[inline]
    pub fn new(chunks: Vec<Array>, dtype: DType) -> Self {
        assert!(
            chunks.iter().map(|chunk| chunk.dtype()).all_equal(),
            "Chunks have differing dtypes"
        );
        let length = chunks.iter().map(|c| c.len()).sum();
        let mut chunk_ends = Vec::<usize>::with_capacity(chunks.len());
        for chunk in chunks.iter() {
            chunk_ends.push(chunk_ends.last().unwrap_or(&0usize) + chunk.len());
        }

        Self {
            chunks,
            chunk_ends,
            dtype,
            offset: 0,
            length,
        }
    }

    #[inline]
    pub fn chunks(&self) -> &[Array] {
        &self.chunks
    }

    pub fn iter_chunks(&self) -> ChunkIterator<'_> {
        ChunkIterator::new(self)
    }

    pub fn into_iter_chunks(self) -> IntoChunkIterator {
        IntoChunkIterator::new(self)
    }

    fn find_physical_location(&self, index: usize) -> (usize, usize) {
        assert!(
            index <= self.offset + self.length,
            "Index out of bounds of the array"
        );
        let offset_index = index + self.offset;
        let index_chunk = self
            .chunk_ends
            .binary_search(&offset_index)
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

pub struct IntoChunkIterator {
    chunks_iter: Box<dyn Iterator<Item = Array>>,
    num_chunks: usize,
    offset_in_first_chunk: Option<usize>,
    length_in_last_chunk: Option<usize>,
    current_chunk_idx: usize,
}

impl IntoChunkIterator {
    fn new(array: ChunkedArray) -> Self {
        if array.chunks.is_empty() {
            return Self {
                chunks_iter: Box::new(iter::empty::<Array>()),
                num_chunks: 0,
                offset_in_first_chunk: None,
                length_in_last_chunk: None,
                current_chunk_idx: 0,
            };
        }

        let array_length = array.len();
        let (offset_chunk, offset_in_first_chunk) = array.find_physical_location(0);
        let (length_chunk, length_in_last_chunk) = array.find_physical_location(array.length);
        Self {
            chunks_iter: Box::new(array.chunks.into_iter().skip(offset_chunk)),
            num_chunks: length_chunk - offset_chunk + 1,
            offset_in_first_chunk: if offset_in_first_chunk == 0 {
                None
            } else {
                Some(offset_in_first_chunk)
            },
            length_in_last_chunk: if array.offset + array.length == array_length {
                None
            } else {
                Some(length_in_last_chunk)
            },
            current_chunk_idx: 0,
        }
    }
}

impl Iterator for IntoChunkIterator {
    type Item = Array;

    fn next(&mut self) -> Option<Self::Item> {
        self.chunks_iter.next().map(|chunk| {
            self.current_chunk_idx += 1;
            if self.num_chunks == 1 {
                if let Some(off) = self.offset_in_first_chunk {
                    if let Some(length) = self.length_in_last_chunk {
                        chunk.slice(off, length)
                    } else {
                        chunk.slice(off, chunk.len())
                    }
                } else if let Some(length) = self.length_in_last_chunk {
                    chunk.slice(0, length)
                } else {
                    chunk
                }
            } else if self.current_chunk_idx == 1 {
                self.offset_in_first_chunk
                    .map(|off| chunk.slice(off, chunk.len()))
                    .unwrap_or(chunk)
            } else if self.current_chunk_idx == self.num_chunks {
                self.length_in_last_chunk
                    .map(|len| chunk.slice(0, len))
                    .unwrap_or(chunk)
            } else {
                chunk
            }
        })
    }
}

pub struct ChunkIterator<'a> {
    chunks_iter: Box<dyn Iterator<Item = &'a Array> + 'a>,
    num_chunks: usize,
    offset_in_first_chunk: Option<usize>,
    length_in_last_chunk: Option<usize>,
    current_chunk_idx: usize,
}

impl<'a> ChunkIterator<'a> {
    fn new(array: &'a ChunkedArray) -> Self {
        if array.chunks.is_empty() {
            return Self {
                chunks_iter: Box::new(iter::empty::<&Array>()),
                num_chunks: 0,
                offset_in_first_chunk: None,
                length_in_last_chunk: None,
                current_chunk_idx: 0,
            };
        }

        let (offset_chunk, offset_in_first_chunk) = array.find_physical_location(0);
        let (length_chunk, length_in_last_chunk) = array.find_physical_location(array.length);
        Self {
            chunks_iter: Box::new(array.chunks.iter().skip(offset_chunk)),
            num_chunks: length_chunk - offset_chunk + 1,
            offset_in_first_chunk: if offset_in_first_chunk == 0 {
                None
            } else {
                Some(offset_in_first_chunk)
            },
            length_in_last_chunk: if array.offset + array.length == array.len() {
                None
            } else {
                Some(length_in_last_chunk)
            },
            current_chunk_idx: 0,
        }
    }
}

impl<'a> Iterator for ChunkIterator<'a> {
    type Item = Array;

    fn next(&mut self) -> Option<Self::Item> {
        self.chunks_iter.next().map(|chunk| {
            self.current_chunk_idx += 1;
            if self.num_chunks == 1 {
                if let Some(off) = self.offset_in_first_chunk {
                    if let Some(length) = self.length_in_last_chunk {
                        chunk.slice(off, length)
                    } else {
                        chunk.slice(off, chunk.len() - off)
                    }
                } else if let Some(length) = self.length_in_last_chunk {
                    chunk.slice(0, length)
                } else {
                    chunk.clone()
                }
            } else if self.current_chunk_idx == 1 {
                self.offset_in_first_chunk
                    .map(|off| chunk.slice(off, chunk.len() - off))
                    .unwrap_or(chunk.clone())
            } else if self.current_chunk_idx == self.num_chunks {
                self.length_in_last_chunk
                    .map(|len| chunk.slice(0, len))
                    .unwrap_or(chunk.clone())
            } else {
                chunk.clone()
            }
        })
    }
}

impl ArrayEncoding for ChunkedArray {
    fn len(&self) -> usize {
        self.length
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
        Box::new(ChunkedArrowIterator::<ChunkIterator<'_>>::new(self))
    }

    fn into_iter_arrow(self) -> Box<IntoArrowIterator> {
        Box::new(ChunkedArrowIterator::<IntoChunkIterator>::new(self))
    }

    fn slice(&self, offset: usize, length: usize) -> Array {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Array {
        let mut cloned = self.clone();
        cloned.offset += offset;
        cloned.length = length;
        Array::Chunked(cloned)
    }
}

struct ChunkedArrowIterator<T: Iterator<Item = Array>> {
    chunks_iter: T,
    arrow_iter: Option<Box<IntoArrowIterator>>,
}

impl<'a> ChunkedArrowIterator<ChunkIterator<'a>> {
    fn new(array: &'a ChunkedArray) -> Self {
        let mut chunks_iter = array.iter_chunks();
        let arrow_iter = chunks_iter.next().map(|c| c.into_iter_arrow());
        Self {
            chunks_iter,
            arrow_iter,
        }
    }
}

impl ChunkedArrowIterator<IntoChunkIterator> {
    fn new(array: ChunkedArray) -> Self {
        let mut chunks_iter = array.into_iter_chunks();
        let arrow_iter = chunks_iter.next().map(|c| c.into_iter_arrow());
        Self {
            chunks_iter,
            arrow_iter,
        }
    }
}

impl<T: Iterator<Item = Array>> Iterator for ChunkedArrowIterator<T> {
    type Item = Box<dyn ArrowArray>;

    fn next(&mut self) -> Option<Self::Item> {
        self.arrow_iter
            .as_mut()
            .and_then(|iter| iter.next())
            .or_else(|| {
                self.chunks_iter.next().and_then(|next_chunk| {
                    self.arrow_iter = Some(next_chunk.into_iter_arrow());
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
    pub fn slice() {
        let arrow_chunk1 = ArrowPrimitiveArray::<u64>::from_vec(vec![1, 2, 3]);
        let arrow_chunk2 = ArrowPrimitiveArray::<u64>::from_vec(vec![4, 5, 6]);
        let chunk1: PrimitiveArray = (&arrow_chunk1).into();
        let chunk2: PrimitiveArray = (&arrow_chunk2).into();
        let chunked = ChunkedArray::new(
            vec![chunk1.into(), chunk2.into()],
            DType::UInt(IntWidth::_64),
        )
        .slice(2, 3);

        chunked
            .iter_arrow()
            .zip(
                vec![
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
}
