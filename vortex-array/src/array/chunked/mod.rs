//! First-class chunked arrays.
//!
//! Vortex is a chunked array library that's able to

use futures_util::stream;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use vortex_dtype::{DType, Nullability, PType};
use vortex_error::{vortex_bail, vortex_panic, VortexExpect as _, VortexResult};
use vortex_scalar::Scalar;

use crate::array::primitive::PrimitiveArray;
use crate::compute::unary::{scalar_at, subtract_scalar, SubtractScalarFn};
use crate::compute::{search_sorted, SearchResult, SearchSortedSide};
use crate::iter::{ArrayIterator, ArrayIteratorAdapter};
use crate::stats::StatsSet;
use crate::stream::{ArrayStream, ArrayStreamAdapter};
use crate::validity::Validity::NonNullable;
use crate::validity::{ArrayValidity, LogicalValidity, Validity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, IntoArray};

mod canonical;
mod compute;
mod stats;
mod variants;

impl_encoding!("vortex.chunked", 11u16, Chunked);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkedMetadata {
    num_chunks: usize,
}

impl ChunkedArray {
    const ENDS_DTYPE: DType = DType::Primitive(PType::U64, Nullability::NonNullable);

    pub fn try_new(chunks: Vec<Array>, dtype: DType) -> VortexResult<Self> {
        for chunk in &chunks {
            if chunk.dtype() != &dtype {
                vortex_bail!(MismatchedTypes: dtype, chunk.dtype());
            }
        }

        let chunk_offsets = [0u64]
            .into_iter()
            .chain(chunks.iter().map(|c| c.len() as u64))
            .scan(0, |acc, c| {
                *acc += c;
                Some(*acc)
            })
            .collect_vec();

        let num_chunks = chunk_offsets.len() - 1;
        let length = *chunk_offsets.last().unwrap_or_else(|| {
            unreachable!("Chunk ends is guaranteed to have at least one element")
        }) as usize;

        let mut children = Vec::with_capacity(chunks.len() + 1);
        children.push(PrimitiveArray::from_vec(chunk_offsets, NonNullable).into_array());
        children.extend(chunks);

        Self::try_from_parts(
            dtype,
            length,
            ChunkedMetadata { num_chunks },
            children.into(),
            StatsSet::new(),
        )
    }

    #[inline]
    pub fn chunk(&self, idx: usize) -> Option<Array> {
        let chunk_start = usize::try_from(&scalar_at(&self.chunk_offsets(), idx).ok()?).ok()?;
        let chunk_end = usize::try_from(&scalar_at(&self.chunk_offsets(), idx + 1).ok()?).ok()?;

        // Offset the index since chunk_ends is child 0.
        self.array()
            .child(idx + 1, self.array().dtype(), chunk_end - chunk_start)
    }

    pub fn nchunks(&self) -> usize {
        self.metadata().num_chunks
    }

    #[inline]
    pub fn chunk_offsets(&self) -> Array {
        self.array()
            .child(0, &Self::ENDS_DTYPE, self.nchunks() + 1)
            .vortex_expect("Missing chunk ends in ChunkedArray")
    }

    pub fn find_chunk_idx(&self, index: usize) -> (usize, usize) {
        assert!(index <= self.len(), "Index out of bounds of the array");

        let search_result = search_sorted(&self.chunk_offsets(), index, SearchSortedSide::Left)
            .vortex_expect("Search sorted failed in find_chunk_idx");
        let index_chunk = match search_result {
            SearchResult::Found(i) => {
                if i == self.nchunks() {
                    i - 1
                } else {
                    i
                }
            }
            SearchResult::NotFound(i) => i - 1,
        };
        let chunk_start = &scalar_at(&self.chunk_offsets(), index_chunk)
            .and_then(|s| usize::try_from(&s))
            .vortex_expect("Failed to find chunk start in find_chunk_idx");

        let index_in_chunk = index - chunk_start;
        (index_chunk, index_in_chunk)
    }

    pub fn chunks(&self) -> impl Iterator<Item = Array> + '_ {
        (0..self.nchunks()).map(|c| {
            self.chunk(c).unwrap_or_else(|| 
                vortex_panic!(
                    "Chunk should {} exist but doesn't (nchunks: {})",
                    c,
                    self.nchunks()
                )
            )
        })
    }

    pub fn array_iterator(&self) -> impl ArrayIterator + '_ {
        ArrayIteratorAdapter::new(self.dtype().clone(), self.chunks().map(Ok))
    }

    pub fn array_stream(&self) -> impl ArrayStream + '_ {
        ArrayStreamAdapter::new(self.dtype().clone(), stream::iter(self.chunks().map(Ok)))
    }
}

impl ArrayTrait for ChunkedArray {}

impl FromIterator<Array> for ChunkedArray {
    fn from_iter<T: IntoIterator<Item = Array>>(iter: T) -> Self {
        let chunks: Vec<Array> = iter.into_iter().collect();
        let dtype = chunks
            .first()
            .map(|c| c.dtype().clone())
            .vortex_expect("Cannot infer DType from an empty iterator");
        Self::try_new(chunks, dtype).vortex_expect("Failed to create chunked array from iterator")
    }
}

impl AcceptArrayVisitor for ChunkedArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("chunk_ends", &self.chunk_offsets())?;
        for (idx, chunk) in self.chunks().enumerate() {
            visitor.visit_child(format!("[{}]", idx).as_str(), &chunk)?;
        }
        Ok(())
    }
}

impl ArrayValidity for ChunkedArray {
    fn is_valid(&self, index: usize) -> bool {
        let (chunk, offset_in_chunk) = self.find_chunk_idx(index);
        self.chunk(chunk)
            .unwrap_or_else(|| vortex_panic!(OutOfBounds: chunk, 0, self.nchunks()))
            .with_dyn(|a| a.is_valid(offset_in_chunk))
    }

    fn logical_validity(&self) -> LogicalValidity {
        let validity = self
            .chunks()
            .map(|a| a.with_dyn(|arr| arr.logical_validity()))
            .collect::<Validity>();
        validity.to_logical(self.len())
    }
}

impl SubtractScalarFn for ChunkedArray {
    fn subtract_scalar(&self, to_subtract: &Scalar) -> VortexResult<Array> {
        let chunks = self
            .chunks()
            .map(|chunk| subtract_scalar(&chunk, to_subtract))
            .collect::<VortexResult<Vec<_>>>()?;
        Ok(Self::try_new(chunks, self.dtype().clone())?.into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::{DType, NativePType, Nullability, PType};

    use crate::array::chunked::ChunkedArray;
    use crate::compute::slice;
    use crate::compute::unary::subtract_scalar;
    use crate::{Array, IntoArray, IntoArrayVariant, ToArray};

    fn chunked_array() -> ChunkedArray {
        ChunkedArray::try_new(
            vec![
                vec![1u64, 2, 3].into_array(),
                vec![4u64, 5, 6].into_array(),
                vec![7u64, 8, 9].into_array(),
            ],
            DType::Primitive(PType::U64, Nullability::NonNullable),
        )
        .unwrap()
    }

    fn assert_equal_slices<T: NativePType>(arr: Array, slice: &[T]) {
        let mut values = Vec::with_capacity(arr.len());
        ChunkedArray::try_from(arr)
            .unwrap()
            .chunks()
            .map(|a| a.into_primitive().unwrap())
            .for_each(|a| values.extend_from_slice(a.maybe_null_slice::<T>()));
        assert_eq!(values, slice);
    }

    #[test]
    pub fn slice_middle() {
        assert_equal_slices(slice(chunked_array().array(), 2, 5).unwrap(), &[3u64, 4, 5])
    }

    #[test]
    pub fn slice_begin() {
        assert_equal_slices(slice(chunked_array().array(), 1, 3).unwrap(), &[2u64, 3]);
    }

    #[test]
    pub fn slice_aligned() {
        assert_equal_slices(slice(chunked_array().array(), 3, 6).unwrap(), &[4u64, 5, 6]);
    }

    #[test]
    pub fn slice_many_aligned() {
        assert_equal_slices(
            slice(chunked_array().array(), 0, 6).unwrap(),
            &[1u64, 2, 3, 4, 5, 6],
        );
    }

    #[test]
    pub fn slice_end() {
        assert_equal_slices(slice(chunked_array().array(), 7, 8).unwrap(), &[8u64]);
    }

    #[test]
    pub fn slice_exactly_end() {
        assert_equal_slices(slice(chunked_array().array(), 6, 9).unwrap(), &[7u64, 8, 9]);
    }

    #[test]
    fn test_scalar_subtract() {
        let chunked = chunked_array();
        let to_subtract = 1u64;
        let array = subtract_scalar(&chunked.to_array(), &to_subtract.into()).unwrap();

        let chunked = ChunkedArray::try_from(array).unwrap();
        let mut chunks_out = chunked.chunks();

        let results = chunks_out
            .next()
            .unwrap()
            .into_primitive()
            .unwrap()
            .maybe_null_slice::<u64>()
            .to_vec();
        assert_eq!(results, &[0u64, 1, 2]);
        let results = chunks_out
            .next()
            .unwrap()
            .into_primitive()
            .unwrap()
            .maybe_null_slice::<u64>()
            .to_vec();
        assert_eq!(results, &[3u64, 4, 5]);
        let results = chunks_out
            .next()
            .unwrap()
            .into_primitive()
            .unwrap()
            .maybe_null_slice::<u64>()
            .to_vec();
        assert_eq!(results, &[6u64, 7, 8]);
    }
}
