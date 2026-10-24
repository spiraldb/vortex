// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! File and chunk decoding setup amortized over all selected pages in an operation.

use std::collections::BTreeMap;

use pco::data_types::Number;
use pco::wrapped::ChunkDecompressor;
use pco::wrapped::FileDecompressor;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::NativePType;
use vortex_array::validity::Validity;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;

use crate::array::vortex_err_from_pco;

pub(crate) struct PageDecoder<T: Number> {
    file: FileDecompressor,
    chunks: BTreeMap<usize, ChunkDecompressor<T>>,
}

impl<T: Number + NativePType> PageDecoder<T> {
    pub fn new(header: &[u8]) -> VortexResult<Self> {
        let (file, _) = FileDecompressor::new(header).map_err(vortex_err_from_pco)?;
        Ok(Self {
            file,
            chunks: BTreeMap::new(),
        })
    }

    pub fn read(
        &mut self,
        chunk: usize,
        chunk_meta: &[u8],
        n_values: usize,
        buffer: &[u8],
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<PrimitiveArray> {
        let chunk = match self.chunks.entry(chunk) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let (decoder, _) = self
                    .file
                    .chunk_decompressor::<T, _>(chunk_meta)
                    .map_err(vortex_err_from_pco)?;
                entry.insert(decoder)
            }
        };
        let mut decoder = chunk
            .page_decompressor(buffer, n_values)
            .map_err(vortex_err_from_pco)?;
        // `read` takes initialized typed storage. Zero initialization also makes a failed or
        // partial corrupt-page decode safe to drop; no uninitialized typed slice is published.
        let mut values = BufferMut::<T>::zeroed_in(n_values, ctx.allocator().clone());
        decoder.read(&mut values).map_err(vortex_err_from_pco)?;
        Ok(PrimitiveArray::new(values.freeze(), Validity::NonNullable))
    }
}
