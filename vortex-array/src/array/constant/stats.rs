use std::collections::HashMap;

use vortex_error::VortexResult;
use vortex_scalar::BoolScalar;

use crate::array::constant::ConstantArray;
use crate::stats::{ArrayStatisticsCompute, Stat, StatsSet};

impl ArrayStatisticsCompute for ConstantArray {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        let mut stats_map = HashMap::from([(Stat::IsConstant, true.into())]);

        if let Ok(b) = BoolScalar::try_from(self.scalar()) {
            let true_count = if b.value().unwrap_or_default() {
                self.len() as u64
            } else {
                0
            };

            stats_map.insert(Stat::TrueCount, true_count.into());
        }

        Ok(StatsSet::from(stats_map))
    }
}
