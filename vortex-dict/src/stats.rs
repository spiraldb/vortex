use vortex::stats::{ArrayStatistics, Stat, StatsCompute, StatsSet};
use vortex_error::VortexResult;

use crate::dict::DictArray;

impl StatsCompute for DictArray {
    fn compute(&self, _stat: Stat) -> VortexResult<StatsSet> {
        let mut stats = StatsSet::default();

        if let Some(rc) = self.codes().statistics().compute(Stat::RunCount) {
            stats.set(Stat::RunCount, rc);
        }
        if let Some(min) = self.values().statistics().compute(Stat::Min) {
            stats.set(Stat::Min, min);
        }
        if let Some(max) = self.values().statistics().compute(Stat::Max) {
            stats.set(Stat::Max, max);
        }
        if let Some(is_constant) = self.codes().statistics().compute(Stat::IsConstant) {
            stats.set(Stat::IsConstant, is_constant);
        }
        if let Some(null_count) = self.codes().statistics().compute(Stat::NullCount) {
            stats.set(Stat::NullCount, null_count);
        }

        // if dictionary is sorted
        if self
            .values()
            .statistics()
            .compute_is_sorted()
            .unwrap_or(false)
        {
            if let Ok(codes_are_sorted) = self.codes().statistics().compute_is_sorted() {
                stats.set(Stat::IsSorted, codes_are_sorted.into());
            }

            if let Ok(codes_are_strict_sorted) =
                self.codes().statistics().compute_is_strict_sorted()
            {
                stats.set(Stat::IsStrictSorted, codes_are_strict_sorted.into());
            }
        }

        Ok(stats)
    }
}
