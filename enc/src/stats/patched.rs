use crate::array::patched::PatchedArray;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for PatchedArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        todo!()
    }
}
