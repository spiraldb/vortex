// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Whole values, such as dictionary values, kept between the scans that share them.

use std::sync::Arc;

use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use vortex_array::ArrayRef;

use crate::plan::physical::PlanByPtr;
use crate::plan::physical::PlanRef;

type CachedValue = Arc<Mutex<Option<ArrayRef>>>;

/// Values a plan produces over its whole domain whatever rows it is compiled for, kept for the
/// later scans that share this cache.
///
/// A dictionary's values are read whole by every reader of its codes, so readers sharing this
/// cache can reuse them across filters, projections, and splits. The caller controls its lifetime;
/// only whole values that a reader requested are retained.
#[derive(Clone, Default)]
pub struct ValueCache(Arc<Mutex<FxHashMap<PlanByPtr, CachedValue>>>);

impl ValueCache {
    /// The values `plan` produced earlier.
    pub(crate) fn get(&self, plan: &PlanRef) -> Option<ArrayRef> {
        self.0
            .lock()
            .get(&PlanByPtr(plan.clone()))
            .and_then(|value| value.lock().clone())
    }

    /// Records the values of `plan`, keeping the first when two race.
    pub(crate) fn insert(&self, plan: &PlanRef, values: ArrayRef) -> ArrayRef {
        self.0
            .lock()
            .entry(PlanByPtr(plan.clone()))
            .or_default()
            .lock()
            .get_or_insert(values)
            .clone()
    }

    /// Shares only these entries. Once the parent cache is dropped, a value lives until the
    /// last cache using its plan is dropped, rather than until every split finishes.
    pub(crate) fn for_plans<'a>(&self, plans: impl Iterator<Item = &'a PlanRef>) -> Self {
        let mut entries = self.0.lock();
        let scoped = plans
            .map(|plan| {
                let key = PlanByPtr(plan.clone());
                let value = entries.entry(key.clone()).or_default();
                (key, Arc::clone(value))
            })
            .collect();
        Self(Arc::new(Mutex::new(scoped)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vortex_array::ArrayRef;
    use vortex_array::IntoArray;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;
    use vortex_session::registry::ReadContext;

    use super::ValueCache;
    use crate::plan::physical::SegmentScanPlan;
    use crate::segments::SegmentId;

    #[test]
    fn scoped_values_live_until_the_last_consumer() -> VortexResult<()> {
        let array = buffer![1_i32, 2, 3].into_array();
        let plans = [0_u32, 1].map(|id| {
            SegmentScanPlan::new(
                array.dtype().clone(),
                array.len() as u64,
                SegmentId::from(id),
                ReadContext::new([]),
                None,
            )
            .into_plan()
        });
        let parent = ValueCache::default();
        let first = parent.for_plans(std::iter::once(&plans[0]));
        let last = parent.for_plans(std::iter::once(&plans[0]));
        let unrelated = parent.for_plans(std::iter::once(&plans[1]));
        drop(parent);

        first.insert(&plans[0], array.clone());
        unrelated.insert(&plans[1], array.clone());
        let entry = Arc::downgrade(first.0.lock().values().next().expect("one entry"));
        drop(first);
        assert!(ArrayRef::ptr_eq(
            &last.get(&plans[0]).expect("shared value"),
            &array,
        ));
        assert!(entry.upgrade().is_some());
        drop(last);
        assert!(entry.upgrade().is_none());
        assert!(unrelated.get(&plans[1]).is_some());
        Ok(())
    }
}
