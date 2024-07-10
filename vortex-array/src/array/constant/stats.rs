use std::collections::HashMap;

use vortex_error::VortexResult;
use vortex_scalar::BoolScalar;

use crate::array::constant::ConstantArray;
use crate::stats::{ArrayStatisticsCompute, Stat, StatsSet};

impl ArrayStatisticsCompute for ConstantArray {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        if let Ok(b) = BoolScalar::try_from(self.scalar()) {
            let true_count = if b.value().unwrap_or(false) {
                self.len() as u64
            } else {
                0
            };
            return Ok(StatsSet::from(HashMap::from([(
                Stat::TrueCount,
                true_count.into(),
            )])));
        }

        Ok(StatsSet::new())
    }
}
