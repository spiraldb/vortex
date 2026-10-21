// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use vortex_array::expr::BoundExpression;
use vortex_error::VortexResult;
use vortex_session::VortexSession;

use crate::LayoutRef;
use crate::plan::physical::PlanRef;
use crate::plan::physical::lower;
use crate::plan::physical::optimize;
use crate::plan::physical::pipeline::ValueCache;
use crate::plan::physical::plan_row_idx_expression;
use crate::scan::execution::FilterPlans;
use crate::scan::execution::SegmentLocation;
use crate::scan::v2::io::SegmentRanges;
use crate::scan::v2::io::segment_ranges;

/// What the scans of one file share when they project and filter alike, as the partitions of
/// one query do: the lowered layout, the optimized plans, the filter's adaptive conjunct order,
/// and whole values such as dictionary values once read.
///
/// Planning a wide projection costs as much as reading a small one, and every partition would
/// otherwise plan it again. Give each scan of a file the same cache with
/// [`ScanBuilder::with_plan_cache`](super::ScanBuilder::with_plan_cache). A cache handed scans
/// of another file starts over.
#[derive(Clone, Default)]
pub struct PlanCache(Arc<Mutex<Cached>>);

#[derive(Default)]
struct Cached {
    /// The layout the rest was planned for.
    layout: Option<LayoutRef>,
    root: Option<PlanRef>,
    ranges: Option<SegmentRanges>,
    values: ValueCache,
    plans: FxHashMap<Key, Planned>,
}

/// A projection and filter, compared whole but hashed by their functions alone: every node of
/// a bound expression carries its dtype, so hashing them all hashes the dtype of a wide struct
/// once per field and costs as much as planning.
#[derive(Clone, PartialEq, Eq)]
struct Key(BoundExpression, Option<BoundExpression>);

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_functions(&self.0, state);
        if let Some(filter) = &self.1 {
            hash_functions(filter, state);
        }
    }
}

fn hash_functions<H: Hasher>(expression: &BoundExpression, state: &mut H) {
    match expression {
        BoundExpression::Root { .. } => state.write_u8(0),
        BoundExpression::Scalar {
            scalar_fn,
            children,
            ..
        } => {
            state.write_u8(1);
            scalar_fn.hash(state);
            state.write_usize(children.len());
            for child in children.iter() {
                hash_functions(child, state);
            }
        }
    }
}

/// The plans of one projection and filter over a file.
#[derive(Clone)]
pub(super) struct Planned {
    pub(super) root: PlanRef,
    pub(super) projection: PlanRef,
    pub(super) filter: Option<Arc<FilterPlans>>,
    /// The projection reads the root as it is.
    pub(super) identity: bool,
    pub(super) ranges: SegmentRanges,
    pub(super) values: ValueCache,
}

impl Planned {
    /// Plans `projection` and `filter` over `layout`.
    fn new(
        root: PlanRef,
        ranges: SegmentRanges,
        values: ValueCache,
        projection: BoundExpression,
        filter: Option<BoundExpression>,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let filter = filter
            .map(|filter| FilterPlans::new(filter, &root, session).map(Arc::new))
            .transpose()?;
        // An identity projection needs no rewrites. Keep the lowered children lazy, including
        // statistics that a projection-only scan never reads.
        let identity = projection.is_root() && projection.dtype() == root.dtype();
        let projection = if identity {
            root.clone()
        } else {
            optimize(plan_row_idx_expression(projection, root.clone())?)?
        };
        Ok(Self {
            root,
            projection,
            filter,
            identity,
            ranges,
            values,
        })
    }

    /// Plans `projection` and `filter` over `layout` for one scan alone.
    pub(super) fn uncached(
        layout: &LayoutRef,
        locations: &[SegmentLocation],
        projection: BoundExpression,
        filter: Option<BoundExpression>,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        Self::new(
            lower(layout)?,
            segment_ranges(locations),
            ValueCache::default(),
            projection,
            filter,
            session,
        )
    }
}

impl PlanCache {
    /// The plans of `projection` and `filter` over `layout`, planned by the first scan that
    /// asks for them.
    pub(super) fn get(
        &self,
        layout: &LayoutRef,
        locations: &[SegmentLocation],
        projection: BoundExpression,
        filter: Option<BoundExpression>,
        session: &VortexSession,
    ) -> VortexResult<Planned> {
        let mut cached = self.0.lock();
        if !cached
            .layout
            .as_ref()
            .is_some_and(|cached| Arc::ptr_eq(cached, layout))
        {
            *cached = Cached {
                layout: Some(Arc::clone(layout)),
                ..Default::default()
            };
        }
        let key = Key(projection, filter);
        if let Some(planned) = cached.plans.get(&key) {
            return Ok(planned.clone());
        }
        let root = match &cached.root {
            Some(root) => root.clone(),
            None => cached.root.insert(lower(layout)?).clone(),
        };
        let ranges = match &cached.ranges {
            Some(ranges) => Arc::clone(ranges),
            None => Arc::clone(cached.ranges.insert(segment_ranges(locations))),
        };
        let planned = Planned::new(
            root,
            ranges,
            cached.values.clone(),
            key.0.clone(),
            key.1.clone(),
            session,
        )?;
        cached.plans.insert(key, planned.clone());
        Ok(planned)
    }
}

#[cfg(test)]
impl PlanCache {
    /// The projections and filters planned.
    pub(super) fn planned(&self) -> usize {
        self.0.lock().plans.len()
    }
}
