use itertools::Itertools;
use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{FlattenFn, FlattenedArray};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::slice::{slice, SliceFn};
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::scalar::Scalar;

mod take;

impl ArrayCompute for ChunkedArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl AsContiguousFn for ChunkedArray {
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
        // Combine all the chunks into one, then call as_contiguous again.
        let chunks = arrays
            .iter()
            .flat_map(|a| a.as_chunked().chunks().iter())
            .cloned()
            .collect_vec();
        as_contiguous(&chunks)
    }
}

impl FlattenFn for ChunkedArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Chunked(self.clone()))
    }
}

impl ScalarAtFn for ChunkedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let (chunk_index, chunk_offset) = self.find_chunk_idx(index);
        scalar_at(self.chunks[chunk_index].as_ref(), chunk_offset)
    }
}

impl SliceFn for ChunkedArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        let (offset_chunk, offset_in_first_chunk) = self.find_chunk_idx(start);
        let (length_chunk, length_in_last_chunk) = self.find_chunk_idx(stop);

        if length_chunk == offset_chunk {
            if let Some(chunk) = self.chunks.get(offset_chunk) {
                return Ok(ChunkedArray::new(
                    vec![slice(chunk, offset_in_first_chunk, length_in_last_chunk)?],
                    self.dtype.clone(),
                )
                .into_array());
            }
        }

        let mut chunks = self.chunks.clone()[offset_chunk..length_chunk + 1].to_vec();
        if let Some(c) = chunks.first_mut() {
            *c = slice(c, offset_in_first_chunk, c.len())?;
        }

        if length_in_last_chunk == 0 {
            chunks.pop();
        } else if let Some(c) = chunks.last_mut() {
            *c = slice(c, 0, length_in_last_chunk)?;
        }

        Ok(ChunkedArray::new(chunks, self.dtype.clone()).into_array())
    }
}
