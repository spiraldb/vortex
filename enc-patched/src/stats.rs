use crate::PatchedArray;
use enc::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for PatchedArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        todo!()
    }
}
