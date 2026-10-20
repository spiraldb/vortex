// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! State derived from a [`ScanFile`](super::ScanFile) that repeated V2 scans can share.

use std::sync::Arc;

use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::ExactBoundExpr;
use vortex_error::VortexResult;

use super::io::SegmentRanges;
use super::io::segment_ranges;
use crate::LayoutRef;
use crate::plan::physical::PlanRef;
use crate::plan::physical::lower;
use crate::plan::physical::optimize;
use crate::plan::physical::pipeline::ValueCache;
use crate::plan::physical::plan_row_idx_expression;
use crate::scan::execution::SegmentLocation;

/// Reusable state for repeated V2 scans of one file, the V2 counterpart of a cached
/// [`LayoutReader`](crate::LayoutReader) tree.
///
/// Every scan otherwise lowers the layout into a fresh physical plan, optimizes its projection
/// over it, indexes every segment's byte range, and decodes the dictionary values it needs. Scans
/// sharing a cache do each of those at most once: the lowered plan keeps the children it has
/// initialized, each distinct projection keeps its optimized plan, and dictionary values stay
/// keyed by the cached plan that produced them.
///
/// A cache describes one file's layout, segment locations, and segment source. Use a new cache
/// when any of them changes. Cached plans and values live as long as the cache, one set per
/// distinct projection.
#[derive(Clone, Default)]
pub struct ScanCache(Arc<Inner>);

#[derive(Default)]
struct Inner {
    root: OnceCell<PlanRef>,
    /// Optimized plans of non-identity projections. Values are cached by the plan that produced
    /// them, so a projection must reuse its plan for its values to be found again.
    projections: Mutex<FxHashMap<ExactBoundExpr, PlanRef>>,
    ranges: OnceCell<SegmentRanges>,
    values: ValueCache,
}

impl ScanCache {
    pub(super) fn root(&self, layout: &LayoutRef) -> VortexResult<PlanRef> {
        self.0.root.get_or_try_init(|| lower(layout)).cloned()
    }

    /// The optimized plan of `projection` over `root`, which must be this cache's root.
    pub(super) fn projection(
        &self,
        projection: BoundExpression,
        root: PlanRef,
    ) -> VortexResult<PlanRef> {
        let key = ExactBoundExpr(projection.clone());
        if let Some(plan) = self.0.projections.lock().get(&key) {
            return Ok(plan.clone());
        }
        let plan = optimize(plan_row_idx_expression(projection, root)?)?;
        Ok(self.0.projections.lock().entry(key).or_insert(plan).clone())
    }

    pub(super) fn ranges(&self, locations: &[SegmentLocation]) -> SegmentRanges {
        Arc::clone(self.0.ranges.get_or_init(|| segment_ranges(locations)))
    }

    pub(super) fn values(&self) -> ValueCache {
        self.0.values.clone()
    }
}
