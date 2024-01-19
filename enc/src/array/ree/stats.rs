use crate::array::ree::REEArray;
use crate::array::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for REEArray {
    fn compute(&self, _stat: Stat) -> StatsSet {
        todo!()
    }
}
