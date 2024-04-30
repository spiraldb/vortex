use std::collections::HashMap;

use vortex::stats::{ArrayStatistics, ArrayStatisticsCompute, Stat};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::dict::DictArray;

impl ArrayStatisticsCompute for DictArray<'_> {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<HashMap<Stat, Scalar>> {
        let mut stats = HashMap::new();

        if let Some(rc) = self.codes().statistics().compute_as(Stat::RunCount) {
            stats.insert(Stat::RunCount, rc);
        }
        if let Some(min) = self.values().statistics().compute_as(Stat::Min) {
            stats.insert(Stat::Min, min);
        }
        if let Some(max) = self.values().statistics().compute_as(Stat::Max) {
            stats.insert(Stat::Max, max);
        }
        if let Some(is_constant) = self.codes().statistics().compute_as(Stat::IsConstant) {
            stats.insert(Stat::IsConstant, is_constant);
        }
        if let Some(null_count) = self.codes().statistics().compute_as(Stat::NullCount) {
            stats.insert(Stat::NullCount, null_count);
        }

        // if dictionary is sorted
        if self
            .values()
            .statistics()
            .compute_as(Stat::IsSorted)
            .unwrap_or(false)
        {
            if let Some(codes_are_sorted) =
                self.codes().statistics().compute_as::<bool>(Stat::IsSorted)
            {
                stats.insert(Stat::IsSorted, codes_are_sorted.into());
            }

            if let Some(codes_are_strict_sorted) = self
                .codes()
                .statistics()
                .compute_as::<bool>(Stat::IsStrictSorted)
            {
                stats.insert(Stat::IsStrictSorted, codes_are_strict_sorted.into());
            }
        }

        Ok(stats)
    }
}
