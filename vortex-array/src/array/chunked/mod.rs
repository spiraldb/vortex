use std::sync::{Arc, RwLock};

use itertools::Itertools;
use linkme::distributed_slice;
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::DType;

use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{ArrayStatistics, OwnedStats, Statistics, StatsSet};
use crate::validity::ArrayValidity;
use crate::validity::Validity;
use crate::{impl_array, ArrayWalker};

mod compute;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct ChunkedArray {
    chunks: Vec<ArrayRef>,
    chunk_ends: Vec<u64>,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl ChunkedArray {
    pub fn new(chunks: Vec<ArrayRef>, dtype: DType) -> Self {
        Self::try_new(chunks, dtype).unwrap()
    }

    pub fn try_new(chunks: Vec<ArrayRef>, dtype: DType) -> VortexResult<Self> {
        for chunk in &chunks {
            if chunk.dtype() != &dtype {
                vortex_bail!(MismatchedTypes: dtype, chunk.dtype());
            }
        }
        let chunk_ends = [0u64]
            .into_iter()
            .chain(chunks.iter().map(|c| c.len() as u64))
            .scan(0, |acc, c| {
                *acc += c;
                Some(*acc)
            })
            .collect_vec();
        Ok(Self {
            chunks,
            chunk_ends,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn chunks(&self) -> &[ArrayRef] {
        &self.chunks
    }

    #[inline]
    pub fn chunk_ends(&self) -> &[u64] {
        &self.chunk_ends
    }

    pub fn find_chunk_idx(&self, index: usize) -> (usize, usize) {
        assert!(index <= self.len(), "Index out of bounds of the array");
        let index_chunk = self
            .chunk_ends
            .binary_search(&(index as u64))
            // Since chunk ends start with 0 whenever value falls in between two ends it's in the chunk that starts the END
            .unwrap_or_else(|o| o - 1);
        let index_in_chunk = index - self.chunk_ends[index_chunk] as usize;
        (index_chunk, index_in_chunk)
    }
}

impl Array for ChunkedArray {
    impl_array!();

    fn len(&self) -> usize {
        self.chunk_ends.last().map(|&i| i as usize).unwrap_or(0)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.chunks.is_empty() || self.len() == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ChunkedEncoding
    }

    fn nbytes(&self) -> usize {
        self.chunks().iter().map(|arr| arr.nbytes()).sum()
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        for chunk in self.chunks() {
            walker.visit_child(chunk)?;
        }
        Ok(())
    }
}

impl FromIterator<ArrayRef> for ChunkedArray {
    fn from_iter<T: IntoIterator<Item = ArrayRef>>(iter: T) -> Self {
        let chunks: Vec<ArrayRef> = iter.into_iter().collect();
        let dtype = chunks
            .first()
            .map(|c| c.dtype().clone())
            .expect("Cannot create a chunked array from an empty iterator");
        Self::new(chunks, dtype)
    }
}

impl ArrayValidity for ChunkedArray {
    fn logical_validity(&self) -> Validity {
        if !self.dtype.is_nullable() {
            return Validity::Valid(self.len());
        }
        Validity::from_iter(self.chunks.iter().map(|chunk| chunk.logical_validity()))
    }

    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }
}

impl OwnedStats for ChunkedArray {
    fn stats_set(&self) -> &RwLock<StatsSet> {
        &self.stats
    }
}

impl ArrayStatistics for ChunkedArray {
    fn statistics(&self) -> &dyn Statistics {
        self
    }
}

impl ArrayDisplay for ChunkedArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        for (i, c) in self.chunks().iter().enumerate() {
            f.new_total_size(c.nbytes(), |f| f.child(&format!("[{}]", i), c.as_ref()))?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ChunkedEncoding;

impl ChunkedEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.chunked");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_CHUNKED: EncodingRef = &ChunkedEncoding;

impl Encoding for ChunkedEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

#[cfg(test)]
mod test {
    use vortex_schema::{DType, IntWidth, Nullability, Signedness};

    use crate::array::chunked::ChunkedArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::IntoArray;
    use crate::array::{Array, ArrayRef};
    use crate::compute::flatten::flatten_primitive;
    use crate::compute::slice::slice;
    use crate::ptype::NativePType;

    fn chunked_array() -> ChunkedArray {
        ChunkedArray::new(
            vec![
                vec![1u64, 2, 3].into_array(),
                vec![4u64, 5, 6].into_array(),
                vec![7u64, 8, 9].into_array(),
            ],
            DType::Int(
                IntWidth::_64,
                Signedness::Unsigned,
                Nullability::NonNullable,
            ),
        )
    }

    fn assert_equal_slices<T: NativePType>(arr: ArrayRef, slice: &[T]) {
        let mut values = Vec::with_capacity(arr.len());
        arr.as_chunked()
            .chunks()
            .iter()
            .map(|a| flatten_primitive(a.as_ref()).unwrap())
            .for_each(|a| values.extend_from_slice(a.typed_data::<T>()));
        assert_eq!(values, slice);
    }

    #[test]
    pub fn slice_middle() {
        assert_equal_slices(slice(&chunked_array(), 2, 5).unwrap(), &[3u64, 4, 5])
    }

    #[test]
    pub fn slice_begin() {
        assert_equal_slices(slice(&chunked_array(), 1, 3).unwrap(), &[2u64, 3]);
    }

    #[test]
    pub fn slice_aligned() {
        assert_equal_slices(slice(&chunked_array(), 3, 6).unwrap(), &[4u64, 5, 6]);
    }

    #[test]
    pub fn slice_many_aligned() {
        assert_equal_slices(
            slice(&chunked_array(), 0, 6).unwrap(),
            &[1u64, 2, 3, 4, 5, 6],
        );
    }

    #[test]
    pub fn slice_end() {
        assert_equal_slices(slice(&chunked_array(), 7, 8).unwrap(), &[8u64]);
    }
}
