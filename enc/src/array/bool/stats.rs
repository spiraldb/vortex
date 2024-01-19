use crate::array::bool::BoolArray;
use crate::array::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for BoolArray {
    fn compute(&self, _stat: Stat) -> StatsSet {
        todo!()
    }
}
