use crate::array::constant::ConstantArray;
use crate::array::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for ConstantArray {
    fn compute(&self, _stat: Stat) -> StatsSet {
        todo!()
    }
}
