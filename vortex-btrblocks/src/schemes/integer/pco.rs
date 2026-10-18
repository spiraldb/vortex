// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Pco (pcodec) integer compression.

use vortex_array::ArrayId;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::dtype::PType;
use vortex_compressor::scheme::CompressionEstimate;
use vortex_compressor::scheme::DeferredEstimate;
use vortex_compressor::scheme::EstimateVerdict;
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

/// Pco (pcodec) compression for integers.
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
        "vortex.int.pco"
    }

    fn matches(&self, canonical: &Canonical) -> bool {
        canonical.dtype().is_int()
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
        data: &ArrayAndStats,
        _compress_ctx: CompressorContext,
        _exec_ctx: &mut ExecutionCtx,
    ) -> CompressionEstimate {
        // Pco does not support I8 or U8.
        if matches!(data.array_as_primitive().ptype(), PType::I8 | PType::U8) {
            return CompressionEstimate::Verdict(EstimateVerdict::Skip);
        }

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

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_array::ArrayId;
    use vortex_pco::pco_v1_id;
    use vortex_pco::pco_v2_id;

    use super::PCO_V1;
    use super::PCO_V2;
    use crate::Scheme;
    use crate::SchemeExt;

    /// Both variants refine to v2 exactly when the v2 ID is allowed.
    #[rstest]
    #[case::neither(false, false, false)]
    #[case::v1(true, false, false)]
    #[case::v2_only(false, true, true)]
    #[case::both(true, true, true)]
    fn refine_picks_v2_when_v2_id_is_allowed(
        #[case] allow_v1: bool,
        #[case] allow_v2: bool,
        #[case] expect_v2: bool,
        #[values(&PCO_V1, &PCO_V2)] scheme: &'static dyn Scheme,
    ) {
        let allowed =
            |id: &ArrayId| (allow_v1 && *id == pco_v1_id()) || (allow_v2 && *id == pco_v2_id());
        let refined = scheme.refine(&allowed);
        assert_eq!(refined.id(), scheme.id());
        let expected: &dyn Scheme = if expect_v2 { &PCO_V2 } else { &PCO_V1 };
        assert_eq!(refined.produced_encodings(), expected.produced_encodings());
    }
}
