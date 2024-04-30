use itertools::Itertools;
use serde::{Deserialize, Serialize};
use vortex_dtype::{Nullability, PType};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::primitive::PrimitiveArray;
use crate::compute::scalar_at::scalar_at;
use crate::compute::scalar_subtract::{subtract_scalar, SubtractScalarFn};
use crate::compute::search_sorted::{search_sorted, SearchSortedSide};
use crate::validity::Validity::NonNullable;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, ArrayDType, ArrayFlatten, IntoArrayData, OwnedArray, ToArrayData};

mod compute;
mod stats;

impl_encoding!("vortex.chunked", Chunked);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkedMetadata;

impl ChunkedArray<'_> {
    const ENDS_DTYPE: DType = DType::Primitive(PType::U64, Nullability::NonNullable);

    pub fn try_new(chunks: Vec<Array>, dtype: DType) -> VortexResult<Self> {
        for chunk in &chunks {
            if chunk.dtype() != &dtype {
                vortex_bail!(MismatchedTypes: dtype, chunk.dtype());
            }
        }

        let chunk_ends = PrimitiveArray::from_vec(
            [0u64]
                .into_iter()
                .chain(chunks.iter().map(|c| c.len() as u64))
                .scan(0, |acc, c| {
                    *acc += c;
                    Some(*acc)
                })
                .collect_vec(),
            NonNullable,
        );

        let mut children = vec![chunk_ends.into_array_data()];
        children.extend(chunks.iter().map(|a| a.to_array_data()));

        Self::try_from_parts(dtype, ChunkedMetadata, children.into(), HashMap::default())
    }

    #[inline]
    pub fn chunk(&self, idx: usize) -> Option<Array> {
        // Offset the index since chunk_ends is child 0.
        self.array().child(idx + 1, self.array().dtype())
    }

    pub fn nchunks(&self) -> usize {
        self.chunk_ends().len() - 1
    }

    #[inline]
    pub fn chunk_ends(&self) -> Array {
        self.array()
            .child(0, &Self::ENDS_DTYPE)
            .expect("missing chunk ends")
    }

    pub fn find_chunk_idx(&self, index: usize) -> (usize, usize) {
        assert!(index <= self.len(), "Index out of bounds of the array");

        // TODO(ngates): migrate to the new search_sorted API to subtract 1 if not exact match.
        let mut index_chunk = search_sorted(&self.chunk_ends(), index, SearchSortedSide::Left)
            .unwrap()
            .to_index();
        let mut chunk_start =
            usize::try_from(scalar_at(&self.chunk_ends(), index_chunk).unwrap()).unwrap();

        if chunk_start != index {
            index_chunk -= 1;
            chunk_start =
                usize::try_from(scalar_at(&self.chunk_ends(), index_chunk).unwrap()).unwrap();
        }

        let index_in_chunk = index - chunk_start;
        (index_chunk, index_in_chunk)
    }
}

impl<'a> ChunkedArray<'a> {
    pub fn chunks(&'a self) -> impl Iterator<Item = Array<'a>> {
        (0..self.nchunks()).map(|c| self.chunk(c).unwrap())
    }
}

impl FromIterator<OwnedArray> for OwnedChunkedArray {
    fn from_iter<T: IntoIterator<Item = OwnedArray>>(iter: T) -> Self {
        let chunks: Vec<OwnedArray> = iter.into_iter().collect();
        let dtype = chunks
            .first()
            .map(|c| c.dtype().clone())
            .expect("Cannot create a chunked array from an empty iterator");
        Self::try_new(chunks, dtype).unwrap()
    }
}

impl ArrayFlatten for ChunkedArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        Ok(Flattened::Chunked(self))
    }
}

impl AcceptArrayVisitor for ChunkedArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("chunk_ends", &self.chunk_ends())?;
        for (idx, chunk) in self.chunks().enumerate() {
            visitor.visit_child(format!("[{}]", idx).as_str(), &chunk)?;
        }
        Ok(())
    }
}

impl ArrayTrait for ChunkedArray<'_> {
    fn len(&self) -> usize {
        usize::try_from(scalar_at(&self.chunk_ends(), self.nchunks()).unwrap()).unwrap()
    }
}

impl ArrayValidity for ChunkedArray<'_> {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }

    fn logical_validity(&self) -> LogicalValidity {
        todo!()
    }
}

impl EncodingCompression for ChunkedEncoding {}

impl SubtractScalarFn for ChunkedArray<'_> {
    fn subtract_scalar(&self, to_subtract: &Scalar) -> VortexResult<OwnedArray> {
        self.chunks()
            .map(|chunk| subtract_scalar(&chunk, to_subtract.clone()))
            .collect::<VortexResult<Vec<_>>>()
            .map(|chunks| {
                ChunkedArray::try_new(chunks, self.dtype().clone())
                    .expect("Mismatched types in chunked array")
                    .into_array()
            })
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::{DType, Nullability};
    use vortex_dtype::{NativePType, PType};

    use crate::array::chunked::{ChunkedArray, OwnedChunkedArray};
    use crate::compute::scalar_subtract::subtract_scalar;
    use crate::{Array, IntoArray, ToArray};

    #[allow(dead_code)]
    fn chunked_array() -> OwnedChunkedArray {
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

    #[allow(dead_code)]
    fn assert_equal_slices<T: NativePType>(arr: Array, slice: &[T]) {
        let mut values = Vec::with_capacity(arr.len());
        ChunkedArray::try_from(arr)
            .unwrap()
            .chunks()
            .map(|a| a.flatten_primitive().unwrap())
            .for_each(|a| values.extend_from_slice(a.typed_data::<T>()));
        assert_eq!(values, slice);
    }

    #[test]
    fn test_scalar_subtract() {
        let chunk1 = vec![1.0f64, 2.0, 3.0].into_array();
        let chunk2 = vec![4.0f64, 5.0, 6.0].into_array();
        let to_subtract = -1f64;

        let chunked = ChunkedArray::from_iter(vec![chunk1, chunk2]);

        let array = subtract_scalar(&chunked.to_array(), to_subtract).unwrap();

        let chunked = ChunkedArray::try_from(array).unwrap();
        let mut chunks_out = chunked.chunks();
        let results = chunks_out
            .next()
            .unwrap()
            .flatten_primitive()
            .unwrap()
            .typed_data::<f64>()
            .to_vec();
        assert_eq!(results, &[2.0f64, 3.0, 4.0]);
        let results = chunks_out
            .next()
            .unwrap()
            .flatten_primitive()
            .unwrap()
            .typed_data::<f64>()
            .to_vec();
        assert_eq!(results, &[5.0f64, 6.0, 7.0]);
    }

    // FIXME(ngates): bring back when slicing is a compute function.
    // #[test]
    // pub fn slice_middle() {
    //     assert_equal_slices(chunked_array().slice(2, 5).unwrap(), &[3u64, 4, 5])
    // }
    //
    // #[test]
    // pub fn slice_begin() {
    //     assert_equal_slices(chunked_array().slice(1, 3).unwrap(), &[2u64, 3]);
    // }
    //
    // #[test]
    // pub fn slice_aligned() {
    //     assert_equal_slices(chunked_array().slice(3, 6).unwrap(), &[4u64, 5, 6]);
    // }
    //
    // #[test]
    // pub fn slice_many_aligned() {
    //     assert_equal_slices(chunked_array().slice(0, 6).unwrap(), &[1u64, 2, 3, 4, 5, 6]);
    // }
    //
    // #[test]
    // pub fn slice_end() {
    //     assert_equal_slices(chunked_array().slice(7, 8).unwrap(), &[8u64]);
    // }
}
