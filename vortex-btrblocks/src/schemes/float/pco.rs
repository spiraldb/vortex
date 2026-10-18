// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Pco (pcodec) float compression.

use vortex_array::ArrayId;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_compressor::scheme::CompressionEstimate;
use vortex_compressor::scheme::DeferredEstimate;
use vortex_error::VortexResult;
use vortex_pco::Pco;
use vortex_pco::PcoOptions;
use vortex_pco::pco_v1_id;
use vortex_pco::pco_v2_id;

use crate::ArrayAndStats;
use crate::CascadingCompressor;
use crate::CompressorContext;
use crate::Scheme;

/// The Pco scheme that writes the frozen `vortex.pco` format with 8,192-value pages.
pub(crate) static PCO_V1: PcoScheme = PcoScheme::v1();

/// The Pco scheme that writes `vortex.pco.v2`: one large page per chunk with a random-access
/// index.
pub(crate) static PCO_V2: PcoScheme = PcoScheme::v2();

/// Pco (pcodec) compression for floats.
///
/// The v1 mode writes 8,192-value pages, the smallest unit a read decodes. The v2 mode writes one
/// page per chunk for a better ratio, plus checkpoints and block statistics so reads still decode
/// only 1,024-value blocks.
///
/// The default uses v1. [`refine`](Scheme::refine) picks v2 when the v2 ID is allowed and v1
/// otherwise.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PcoScheme {
    options: PcoOptions,
}

impl PcoScheme {
    /// Creates a Pco scheme configured for the frozen v1 format.
    pub const fn v1() -> Self {
        Self {
            options: PcoOptions::v1(pco::DEFAULT_COMPRESSION_LEVEL, 8192),
        }
    }

    /// Creates a Pco scheme configured for the v2 format.
    pub const fn v2() -> Self {
        Self {
            options: PcoOptions::v2(),
        }
    }
}

impl Default for PcoScheme {
    fn default() -> Self {
        Self::v1()
    }
}

impl Scheme for PcoScheme {
    fn scheme_name(&self) -> &'static str {
        "vortex.float.pco"
    }

    fn matches(&self, canonical: &Canonical) -> bool {
        canonical.dtype().is_float()
    }

    fn produced_encodings(&self) -> Vec<ArrayId> {
        if self.options.checkpoint_interval.is_some() {
            vec![pco_v1_id(), pco_v2_id()]
        } else {
            vec![pco_v1_id()]
        }
    }

    fn refine(&self, allowed: &dyn Fn(&ArrayId) -> bool) -> &dyn Scheme {
        if allowed(&pco_v2_id()) {
            &PCO_V2
        } else {
            &PCO_V1
        }
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
        _compressor: &CascadingCompressor,
        data: &ArrayAndStats,
        _compress_ctx: CompressorContext,
        exec_ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        Ok(Pco::compress(data.array_as_primitive(), &self.options, exec_ctx)?.into_array())
    }
}
