// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! State derived from a [`ScanFile`](super::ScanFile) that repeated V2 scans can share.

use std::sync::Arc;
use std::sync::Weak;

use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::ExactBoundExpr;
use vortex_error::VortexResult;

use super::io::SegmentRanges;
use crate::plan::physical::PlanRef;
use crate::plan::physical::pipeline::ValueCache;
use crate::scan::execution::FilterPlans;

/// Reusable state for repeated V2 scans of one file, the V2 counterpart of a cached
/// [`LayoutReader`](crate::LayoutReader) tree.
///
/// Every scan otherwise lowers the layout into a fresh physical plan, optimizes its projection
/// over it, indexes every segment's byte range, and decodes the dictionary values it needs. A scan
/// that finds an entry here reuses it, and one that misses builds the entry and keeps it for later
/// scans. The lowered plan keeps the children it has initialized, each distinct projection keeps
/// its optimized plan, each distinct filter keeps its conjunct and pruning plans along with the
/// zones those plans have already excluded, and dictionary values stay keyed by the cached plan
/// that produced them.
///
/// A cache describes one file's layout, segment locations, and segment source. Use a new cache
/// when any of them changes. Cached plans and values live as long as the cache, one set per
/// distinct projection.
#[derive(Clone, Default)]
pub struct ScanCache(Arc<Inner>);

#[derive(Default)]
struct Inner {
    root: Mutex<Option<PlanRef>>,
    /// Optimized plans of projections, each built once: the scan that gets to a projection
    /// first builds its plan while the others wait for that plan alone. Values are cached by
    /// the plan that produced them, so a projection must reuse its plan for its values to be
    /// found again.
    projections: Mutex<FxHashMap<ExactBoundExpr, Arc<OnceCell<PlanRef>>>>,
    ranges: Mutex<Option<SegmentRanges>>,
    /// Plans of filters, keyed by the exact filter expression, each built once like a
    /// projection's. A pruning plan remembers the zones it excluded, so sharing the plans prunes
    /// a file's zones once for every scan of that filter.
    filters: Mutex<FxHashMap<ExactBoundExpr, Arc<OnceCell<Arc<FilterPlans>>>>>,
    values: ValueCache,
}

impl ScanCache {
    /// The lowered root plan an earlier scan kept.
    pub(super) fn root(&self) -> Option<PlanRef> {
        self.0.root.lock().clone()
    }

    /// Keeps `root`, returning the plan already kept if another scan got there first.
    pub(super) fn insert_root(&self, root: PlanRef) -> PlanRef {
        self.0.root.lock().get_or_insert(root).clone()
    }

    /// The optimized plan of `projection`, built by `build` if no scan has built it yet. Scans
    /// of one projection opening together build it once: the others wait for it, not for the
    /// cache.
    pub(super) fn projection(
        &self,
        projection: &BoundExpression,
        build: impl FnOnce() -> VortexResult<PlanRef>,
    ) -> VortexResult<PlanRef> {
        let cell = Arc::clone(
            self.0
                .projections
                .lock()
                .entry(ExactBoundExpr(projection.clone()))
                .or_default(),
        );
        cell.get_or_try_init(build).cloned()
    }

    /// The segment byte-range index an earlier scan kept.
    pub(super) fn ranges(&self) -> Option<SegmentRanges> {
        self.0.ranges.lock().clone()
    }

    /// Keeps `ranges`, returning the index already kept if another scan got there first.
    pub(super) fn insert_ranges(&self, ranges: SegmentRanges) -> SegmentRanges {
        Arc::clone(self.0.ranges.lock().get_or_insert(ranges))
    }

    /// The plans of `filter` an earlier scan kept, if any.
    #[cfg(test)]
    pub(super) fn filter_plans(&self, filter: &BoundExpression) -> Option<Arc<FilterPlans>> {
        self.0
            .filters
            .lock()
            .get(&ExactBoundExpr(filter.clone()))
            .and_then(|cell| cell.get().cloned())
    }

    /// The plans of `filter`, built by `build` if no scan has built them yet, once for every
    /// scan of the filter opening together.
    pub(super) fn filter_plans_or_build(
        &self,
        filter: &BoundExpression,
        build: impl FnOnce() -> VortexResult<Arc<FilterPlans>>,
    ) -> VortexResult<Arc<FilterPlans>> {
        let cell = Arc::clone(
            self.0
                .filters
                .lock()
                .entry(ExactBoundExpr(filter.clone()))
                .or_default(),
        );
        cell.get_or_try_init(build).cloned()
    }

    pub(super) fn values(&self) -> ValueCache {
        self.0.values.clone()
    }

    /// A handle that finds this cache while a scan holds it, without keeping it alive itself.
    pub fn downgrade(&self) -> WeakScanCache {
        WeakScanCache(Arc::downgrade(&self.0))
    }
}

/// A [`ScanCache`] handle that does not keep the cache alive. Engines that open a file once per
/// partition keep one per file, so concurrent partitions share a cache that is freed with the
/// last scan holding it.
#[derive(Clone)]
pub struct WeakScanCache(Weak<Inner>);

impl WeakScanCache {
    /// The cache, while a scan still holds it.
    pub fn upgrade(&self) -> Option<ScanCache> {
        self.0.upgrade().map(ScanCache)
    }
}
