use crate::array::stats::{Stat, StatsCompute, StatsSet};
use crate::array::varbin::VarBinArray;

impl StatsCompute for VarBinArray {
    fn compute(&self, _stat: Stat) -> StatsSet {
        todo!()
    }
}
