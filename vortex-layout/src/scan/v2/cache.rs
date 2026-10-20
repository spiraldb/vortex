// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! State derived from a [`ScanFile`](super::ScanFile) that repeated V2 scans can share.

use std::sync::Arc;

use once_cell::sync::OnceCell;
use vortex_error::VortexResult;

use super::io::SegmentRanges;
use super::io::segment_ranges;
use super::supports_layout;
use crate::LayoutRef;
use crate::plan::physical::PlanRef;
use crate::plan::physical::lower;
use crate::plan::physical::pipeline::ValueCache;
use crate::scan::execution::SegmentLocation;

/// Reusable state for repeated V2 scans of one file, the V2 counterpart of a cached
/// [`LayoutReader`](crate::LayoutReader) tree.
///
/// Every scan otherwise checks the whole layout tree for support, lowers the layout into a fresh
/// physical plan, indexes every segment's byte range, and decodes the dictionary values it needs.
/// Scans sharing a cache do each of those at most once: the lowered plan keeps the children it
/// has initialized, and dictionary values stay keyed by the cached plan that produced them.
///
/// A cache describes one file's layout, segment locations, and segment source. Use a new cache
/// when any of them changes. Cached values live as long as the cache.
#[derive(Clone, Default)]
pub struct ScanCache(Arc<Inner>);

#[derive(Default)]
struct Inner {
    /// Whether the layout is supported, for non-identity and identity projections.
    supported: [OnceCell<bool>; 2],
    root: OnceCell<PlanRef>,
    ranges: OnceCell<SegmentRanges>,
    values: ValueCache,
}

impl ScanCache {
    pub(super) fn supports_layout(&self, layout: &LayoutRef, identity: bool) -> VortexResult<bool> {
        self.0.supported[usize::from(identity)]
            .get_or_try_init(|| supports_layout(layout, identity))
            .copied()
    }

    pub(super) fn root(&self, layout: &LayoutRef) -> VortexResult<PlanRef> {
        self.0.root.get_or_try_init(|| lower(layout)).cloned()
    }

    pub(super) fn ranges(&self, locations: &[SegmentLocation]) -> SegmentRanges {
        Arc::clone(self.0.ranges.get_or_init(|| segment_ranges(locations)))
    }

    pub(super) fn values(&self) -> ValueCache {
        self.0.values.clone()
    }
}
