// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Whole values, such as dictionary values, and whole decoded segments, kept between the scans
//! that share them.

use std::sync::Arc;

use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use vortex_array::ArrayRef;

use crate::arrays::shared::Shared;
use crate::arrays::shared::SharedArrayExt;
use crate::arrays::shared::SharedArraySlotsExt;
use crate::plan::physical::PlanByPtr;
use crate::plan::physical::PlanRef;
use crate::segments::SegmentId;

/// The bytes a [`ValueCache`] holds unless given another budget.
pub const DEFAULT_VALUE_CACHE_BYTES: u64 = 64 << 20;

/// Values a plan produces over its whole domain whatever rows it is compiled for, kept for the
/// later scans that share this cache.
///
/// A dictionary's values are read whole by every reader of its codes, so readers sharing this
/// cache can reuse them across filters, projections, and splits. Only whole values that a reader
/// requested are retained, and only up to a budget of bytes: the values used least recently are
/// dropped to make room, and values larger than the budget are never kept, so the cache holds
/// at most its budget however much a file holds.
#[derive(Clone)]
pub struct ValueCache(Arc<Mutex<Values>>);

struct Values {
    budget: u64,
    entries: FxHashMap<PlanByPtr, Entry>,
    /// Advances on every use, ordering the entries by when they were last used.
    clock: u64,
}

struct Entry {
    value: ArrayRef,
    used: u64,
}

impl Default for ValueCache {
    fn default() -> Self {
        Self::with_budget(DEFAULT_VALUE_CACHE_BYTES)
    }
}

impl ValueCache {
    /// A cache holding at most `budget` bytes of values.
    pub fn with_budget(budget: u64) -> Self {
        Self(Arc::new(Mutex::new(Values {
            budget,
            entries: FxHashMap::default(),
            clock: 0,
        })))
    }

    /// The values `plan` produced earlier.
    pub(crate) fn get(&self, plan: &PlanRef) -> Option<ArrayRef> {
        let mut values = self.0.lock();
        values.clock += 1;
        let clock = values.clock;
        let entry = values.entries.get_mut(&PlanByPtr(plan.clone()))?;
        entry.used = clock;
        Some(entry.value.clone())
    }

    /// Records the values of `plan`, keeping the first when two race, and returns the values
    /// recorded. Values the budget cannot hold are returned without being kept.
    pub(crate) fn insert(&self, plan: &PlanRef, value: ArrayRef) -> ArrayRef {
        let mut values = self.0.lock();
        values.clock += 1;
        let clock = values.clock;
        let key = PlanByPtr(plan.clone());
        if let Some(entry) = values.entries.get_mut(&key) {
            entry.used = clock;
            return entry.value.clone();
        }
        let bytes = held_bytes(&value);
        if bytes > values.budget {
            return value;
        }
        // Values kept as shared arrays grow once a reader decodes them, so every entry is
        // measured again rather than by the size it was kept at.
        let mut held: Vec<(u64, u64, PlanByPtr)> = values
            .entries
            .iter()
            .map(|(key, entry)| (entry.used, held_bytes(&entry.value), key.clone()))
            .collect();
        let mut total: u64 = held.iter().map(|(_, bytes, _)| bytes).sum::<u64>() + bytes;
        if total > values.budget {
            held.sort_unstable_by_key(|(used, ..)| *used);
            for (_, entry_bytes, key) in held {
                if total <= values.budget {
                    break;
                }
                values.entries.remove(&key);
                total -= entry_bytes;
            }
        }
        values.entries.insert(
            key,
            Entry {
                value: value.clone(),
                used: clock,
            },
        );
        value
    }
}

/// Whole decoded segments kept between the scans of one split.
///
/// A split runs a scan per filter stage and per projection morsel, and each decodes every
/// segment it reads, so a chunk read by two conjuncts, by a conjunct and the projection, or by
/// several morsels was read and decoded again by each. Scans given the same cache read and decode
/// each segment once. A decoded segment holds the bytes the split's IO session keeps for its
/// whole life anyway, so the cache lives as long as the split and needs no budget of its own.
#[derive(Clone, Default)]
pub(crate) struct DecodedSegments(Arc<Mutex<FxHashMap<SegmentId, ArrayRef>>>);

impl DecodedSegments {
    /// The whole decoded segment `id`, if a scan of the split decoded it.
    pub(crate) fn get(&self, id: SegmentId) -> Option<ArrayRef> {
        self.0.lock().get(&id).cloned()
    }

    /// Keeps `array`, the whole decoded segment `id`, for the split's later scans.
    pub(crate) fn insert(&self, id: SegmentId, array: ArrayRef) {
        self.0.lock().entry(id).or_insert(array);
    }
}

/// The bytes `value` holds: a shared array's source and, once decoded, its decoded form too.
fn held_bytes(value: &ArrayRef) -> u64 {
    match value.as_opt::<Shared>() {
        Some(shared) => {
            let source = shared.source().nbytes();
            let current = shared.current_array_ref();
            if ArrayRef::ptr_eq(current, shared.source()) {
                source
            } else {
                source + current.nbytes()
            }
        }
        None => value.nbytes(),
    }
}

#[cfg(test)]
mod tests {
    use vortex_array::IntoArray;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_buffer::Buffer;
    use vortex_session::registry::ReadContext;

    use super::ValueCache;
    use crate::plan::physical::PlanRef;
    use crate::plan::physical::SegmentScanPlan;
    use crate::segments::SegmentId;

    fn plan(segment: u32) -> PlanRef {
        SegmentScanPlan::new(
            DType::Primitive(PType::U8, Nullability::NonNullable),
            1,
            SegmentId::from(segment),
            ReadContext::new([]),
            None,
        )
        .into_plan()
    }

    fn kilobytes(n: usize) -> vortex_array::ArrayRef {
        Buffer::from_iter((0..n * 1_000).map(|i| u8::try_from(i % 251).unwrap_or(0))).into_array()
    }

    /// Values beyond the budget push out those used least recently, and values larger than the
    /// whole budget are never kept.
    #[test]
    fn values_are_kept_within_the_budget() {
        let cache = ValueCache::with_budget(2_000);
        let (first, second, third, big) = (plan(0), plan(1), plan(2), plan(3));
        cache.insert(&first, kilobytes(1));
        cache.insert(&second, kilobytes(1));
        assert!(cache.get(&first).is_some(), "both fit the budget");
        cache.insert(&third, kilobytes(1));
        assert!(
            cache.get(&first).is_some(),
            "the first was used after the second"
        );
        assert!(
            cache.get(&second).is_none(),
            "the second, the least recently used, made room"
        );
        assert!(cache.get(&third).is_some());
        cache.insert(&big, kilobytes(3));
        assert!(
            cache.get(&big).is_none(),
            "a value over the budget is not kept"
        );
        assert!(cache.get(&first).is_some() && cache.get(&third).is_some());
    }
}
