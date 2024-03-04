use vortex::error::VortexResult;

use crate::FFORArray;
use vortex::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for FFORArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::default())
    }
}
