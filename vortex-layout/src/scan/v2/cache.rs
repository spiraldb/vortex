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
/// its optimized plan, each distinct filter keeps its conjunct and zone-proof plans along with the
/// zones those proofs have already excluded, and dictionary values stay keyed by the cached plan
/// that produced them.
///
/// A cache describes one file's layout, segment locations, and segment source. Use a new cache
/// when any of them changes. Cached plans and values live as long as the cache, one set per
/// distinct projection.
#[derive(Clone, Default)]
pub struct ScanCache(Arc<Inner>);

#[derive(Default)]
struct Inner {
    root: OnceCell<PlanRef>,
    /// Optimized plans of projections. Values are cached by the plan that produced them, so a
    /// projection must reuse its plan for its values to be found again.
    projections: Mutex<FxHashMap<ExactBoundExpr, Arc<OnceCell<PlanRef>>>>,
    ranges: OnceCell<SegmentRanges>,
    /// Plans of filters, keyed by the exact filter expression. A proof plan remembers the zones it
    /// excluded, so sharing the plans proves a file's zones once for every scan of that filter.
    filters: Mutex<FxHashMap<ExactBoundExpr, Arc<OnceCell<Arc<FilterPlans>>>>>,
    values: ValueCache,
}

impl ScanCache {
    /// The lowered root plan, lowered by `lower` unless an earlier scan kept it. Scans that miss
    /// together wait for one of them to lower it.
    pub(super) fn root_or_try_init(
        &self,
        lower: impl FnOnce() -> VortexResult<PlanRef>,
    ) -> VortexResult<PlanRef> {
        self.0.root.get_or_try_init(lower).cloned()
    }

    /// The optimized plan of `projection`, planned by `plan` unless an earlier scan kept it.
    pub(super) fn projection_or_try_init(
        &self,
        projection: &BoundExpression,
        plan: impl FnOnce() -> VortexResult<PlanRef>,
    ) -> VortexResult<PlanRef> {
        cell(&self.0.projections, projection)
            .get_or_try_init(plan)
            .cloned()
    }

    /// The segment byte-range index, built by `index` unless an earlier scan kept it.
    pub(super) fn ranges_or_init(&self, index: impl FnOnce() -> SegmentRanges) -> SegmentRanges {
        Arc::clone(self.0.ranges.get_or_init(index))
    }

    /// The plans of `filter` an earlier scan kept.
    #[cfg(test)]
    pub(super) fn filter_plans(&self, filter: &BoundExpression) -> Option<Arc<FilterPlans>> {
        self.0
            .filters
            .lock()
            .get(&ExactBoundExpr(filter.clone()))
            .and_then(|plans| plans.get().cloned())
    }

    /// The plans of `filter`, planned by `plan` unless an earlier scan kept them.
    pub(super) fn filter_plans_or_try_init(
        &self,
        filter: &BoundExpression,
        plan: impl FnOnce() -> VortexResult<FilterPlans>,
    ) -> VortexResult<Arc<FilterPlans>> {
        cell(&self.0.filters, filter)
            .get_or_try_init(|| plan().map(Arc::new))
            .cloned()
    }

    pub(super) fn values(&self) -> ValueCache {
        self.0.values.clone()
    }

    /// A handle that finds this cache while a scan holds it, without keeping it alive itself.
    pub fn downgrade(&self) -> WeakScanCache {
        WeakScanCache(Arc::downgrade(&self.0))
    }
}

/// The cell of `key` in `map`, so scans that miss together build its entry once.
fn cell<T>(
    map: &Mutex<FxHashMap<ExactBoundExpr, Arc<OnceCell<T>>>>,
    key: &BoundExpression,
) -> Arc<OnceCell<T>> {
    Arc::clone(map.lock().entry(ExactBoundExpr(key.clone())).or_default())
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
