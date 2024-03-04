use crate::REEArray;
use vortex::error::VortexResult;
use vortex::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for REEArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        todo!()
    }
}
