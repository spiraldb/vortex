// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Offset-based storage for binary and UTF-8 arrays.
//!
//! Canonical binary and UTF-8 arrays use
//! [`VarBinViewArray`](vortex_array::arrays::VarBinViewArray), which spends a fixed 16 bytes per
//! element on an opaque views buffer that no scheme can compress.
//! Re-encoding as [`VarBinArray`] replaces that buffer with an offsets child array, which the
//! cascading compressor can then compress with the ordinary integer schemes. For fixed-width
//! values the offsets are a constant-stride sequence and collapse to nothing.

use vortex_array::ArrayId;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VTable;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBin;
use vortex_array::arrays::VarBinArray;
use vortex_array::arrays::primitive::PrimitiveArrayExt;
use vortex_array::arrays::varbin::VarBinArraySlotsExt;
use vortex_array::builders::VarBinBuilder;
use vortex_compressor::scheme::CompressionEstimate;
use vortex_compressor::scheme::DeferredEstimate;
use vortex_compressor::scheme::SchemeExt;
use vortex_error::VortexResult;

use crate::ArrayAndStats;
use crate::CascadingCompressor;
use crate::CompressorContext;
use crate::Scheme;

/// Offset-based storage with compressed offsets and unchanged binary or UTF-8 payloads.
///
/// Competes with payload-compressing schemes through sampled size estimates. The compressor
/// retains the canonical view representation if the selected result does not save space.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct VarBinScheme {
    utf8: bool,
}

impl VarBinScheme {
    /// Offset-based storage for binary arrays.
    pub const BINARY: Self = Self { utf8: false };

    /// Offset-based storage for UTF-8 arrays.
    pub const UTF8: Self = Self { utf8: true };
}

impl Scheme for VarBinScheme {
    fn scheme_name(&self) -> &'static str {
        if self.utf8 {
            "vortex.string.varbin"
        } else {
            "vortex.binary.varbin"
        }
    }

    fn matches(&self, canonical: &Canonical) -> bool {
        if self.utf8 {
            canonical.dtype().is_utf8()
        } else {
            canonical.dtype().is_binary()
        }
    }

    fn produced_encodings(&self) -> Vec<ArrayId> {
        vec![VarBin.id()]
    }

    fn num_children(&self) -> usize {
        1
    }

    fn expected_compression_ratio(
        &self,
        _data: &ArrayAndStats,
        _compress_ctx: CompressorContext,
        _exec_ctx: &mut ExecutionCtx,
    ) -> CompressionEstimate {
        CompressionEstimate::Deferred(DeferredEstimate::Sample)
    }

    fn compress(
        &self,
        compressor: &CascadingCompressor,
        data: &ArrayAndStats,
        compress_ctx: CompressorContext,
        exec_ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        // `append_to_builder` resolves the views slice and data buffers once and appends
        // borrowed slices into a single pre-sized allocation. Iterating the array per element
        // instead would clone a buffer handle and allocate for every value.
        let array = data.array();
        let mut builder = VarBinBuilder::<u64>::with_capacity_in(
            array.dtype().clone(),
            array.len(),
            exec_ctx.allocator(),
        );
        array.append_to_builder(&mut builder, exec_ctx)?;
        let varbin = builder.finish_into_varbin();

        let offsets = varbin
            .offsets()
            .clone()
            .execute::<PrimitiveArray>(exec_ctx)?
            .narrow(exec_ctx)?
            .into_array();
        let compressed_offsets =
            compressor.compress_child(&offsets, &compress_ctx, self.id(), 0, exec_ctx)?;

        Ok(VarBinArray::try_new(
            compressed_offsets,
            varbin.bytes().clone(),
            varbin.dtype().clone(),
            varbin.validity()?,
        )?
        .into_array())
    }
}
