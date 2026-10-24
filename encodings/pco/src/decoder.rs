// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! File and chunk decoding setup amortized over all selected pages in an operation.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use pco::data_types::Number;
use pco::wrapped::ChunkDecompressor;
use pco::wrapped::FileDecompressor;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::NativePType;
use vortex_array::validity::Validity;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;

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
        let mut values = BufferMut::<T>::zeroed_in(n_values, ctx.allocator().clone());
        self.read_into(chunk, chunk_meta, n_values, buffer, &mut values)?;
        Ok(PrimitiveArray::new(values.freeze(), Validity::NonNullable))
    }
    /// Decode exactly one complete page into caller-owned initialized storage.
    /// A corrupt/partial page never publishes the caller's unfinished output array.
    pub fn read_into(
        &mut self,
        chunk: usize,
        chunk_meta: &[u8],
        n_values: usize,
        buffer: &[u8],
        output: &mut [T],
    ) -> VortexResult<()> {
        vortex_ensure_eq!(output.len(), n_values, "Pco page output length mismatch");
        let chunk = match self.chunks.entry(chunk) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
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
        let progress = decoder.read(output).map_err(vortex_err_from_pco)?;
        vortex_ensure_eq!(
            progress.n_processed,
            n_values,
            "Pco page did not fill output"
        );
        vortex_ensure!(progress.finished, "Pco page decode is incomplete");
        Ok(())
    }
}
