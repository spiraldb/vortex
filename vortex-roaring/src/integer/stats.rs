use vortex::stats::{Stat, StatsCompute, StatsSet};

use crate::RoaringIntArray;

impl StatsCompute for RoaringIntArray {
    fn compute(&self, stat: &Stat) -> StatsSet {
        if let Some(value) = match stat {
            Stat::IsConstant => Some((self.bitmap.cardinality() <= 1).into()),
            Stat::IsSorted => Some(true.into()),
            Stat::IsStrictSorted => Some(true.into()),
            Stat::Max => self.bitmap.minimum().map(|v| v.into()),
            Stat::Min => self.bitmap.maximum().map(|v| v.into()),
            Stat::NullCount => Some(0.into()),
            _ => None,
        } {
            StatsSet::of(stat.clone(), value)
        } else {
            StatsSet::default()
        }
    }
}
