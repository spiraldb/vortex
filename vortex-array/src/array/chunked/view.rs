use crate::array::chunked::{ChunkedArray, ChunkedEncoding};
use crate::array::IntoArray;
use crate::array::{Array, ArrayRef};
use crate::compute::cast::cast;
use crate::compute::flatten::flatten_primitive;
use crate::compute::take::{take, TakeFn};
use crate::compute::ArrayCompute;
use crate::ptype::PType;
use crate::serde::vtable::ComputeVTable;
use crate::serde::ArrayView;
use vortex_error::VortexResult;

// Need to design some trait that can be satisfied by both the ChunkedView and the ChunkedArray,
// and can be used to implement all compute.
pub trait ChunkedTrait: Array {
    // Some way to iterate over chunks as &dyn Array.
    // But how without heap-allocating an iterator?
    // Or we could do nchunks + with_chunk(idx, Fn(&dyn Array) -> T)
    fn nchunks(&self) -> usize;

    /// TODO(ngates): we can't easily do this and use ChunkedTrait as an object trait.
    ///  So what if we use a stack-allocated closure instead of a generic?
    // fn with_chunk<F, T>(&self, idx: usize, f: F) -> T
    // where
    //     F: FnOnce(&dyn Array) -> T;
    fn map_chunk(
        &self,
        idx: usize,
        f: &dyn Fn(&dyn Array) -> VortexResult<ArrayRef>,
    ) -> VortexResult<ArrayRef>;

    // Some way to get chunk lengths.
    fn chunk_ends(&self) -> &[u64];

    fn find_chunk_idx(&self, index: usize) -> (usize, usize) {
        assert!(index <= self.len(), "Index out of bounds of the array");
        let index_chunk = self
            .chunk_ends()
            .binary_search(&(index as u64))
            // If the result of binary_search is Ok it means we have exact match, since these are chunk ends EXCLUSIVE we have to add one to move to the next one
            .map(|o| o + 1)
            .unwrap_or_else(|o| o);
        let index_in_chunk = index
            - if index_chunk == 0 {
                0
            } else {
                self.chunk_ends()[index_chunk - 1]
            } as usize;
        (index_chunk, index_in_chunk)
    }
}

impl<'a> ChunkedTrait for ArrayView<'a> {
    fn nchunks(&self) -> usize {
        self.nchildren()
    }

    fn map_chunk(
        &self,
        idx: usize,
        f: &dyn Fn(&dyn Array) -> VortexResult<ArrayRef>,
    ) -> VortexResult<ArrayRef> {
        let child = self.child(idx, self.dtype()).unwrap();
        (&f)(&child)
    }

    // fn with_chunk<F, T>(&self, idx: usize, f: F) -> T
    // where
    //     F: FnOnce(&dyn Array) -> T,
    // {
    //     let child = self.child(idx, self.dtype()).unwrap();
    //     f(&child)
    // }

    fn chunk_ends(&self) -> &[u64] {
        todo!()
    }
}

impl<'view> ComputeVTable<ArrayView<'view>> for ChunkedEncoding {
    fn take(&self) -> Option<&dyn crate::serde::vtable::TakeFn<ArrayView<'view>>> {
        Some(self)
    }
}

impl<'view> crate::serde::vtable::TakeFn<ArrayView<'view>> for ChunkedEncoding {
    fn take(&self, array: &ArrayView<'view>, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let chunked: &dyn ChunkedTrait = array;
        TakeFn::take(&chunked, indices)
    }
}

impl ArrayCompute for &dyn ChunkedTrait {
    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl TakeFn for &dyn ChunkedTrait {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        if self.len() == indices.len() {
            return Ok(self.to_array());
        }

        let indices = flatten_primitive(cast(indices, PType::U64.into())?.as_ref())?;

        // While the chunk idx remains the same, accumulate a list of chunk indices.
        let mut chunks = Vec::new();
        let mut indices_in_chunk = Vec::new();
        let mut prev_chunk_idx = self
            .find_chunk_idx(indices.typed_data::<u64>()[0] as usize)
            .0;
        for idx in indices.typed_data::<u64>() {
            let (chunk_idx, idx_in_chunk) = self.find_chunk_idx(*idx as usize);

            if chunk_idx != prev_chunk_idx {
                // Start a new chunk
                let indices_in_chunk_array = indices_in_chunk.clone().into_array();
                chunks.push(self.map_chunk(prev_chunk_idx, &|c| take(c, &indices_in_chunk_array))?);
                indices_in_chunk = Vec::new();
            }

            indices_in_chunk.push(idx_in_chunk as u64);
            prev_chunk_idx = chunk_idx;
        }

        if !indices_in_chunk.is_empty() {
            let indices_in_chunk_array = indices_in_chunk.into_array();
            chunks.push(self.map_chunk(prev_chunk_idx, &|c| take(c, &indices_in_chunk_array))?);
        }

        Ok(ChunkedArray::new(chunks, self.dtype().clone()).into_array())
    }
}
