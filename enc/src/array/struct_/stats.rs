use crate::array::stats::{Stat, StatsCompute, StatsSet};
use crate::array::struct_::StructArray;

impl StatsCompute for StructArray {
    fn compute(&self, _stat: Stat) -> StatsSet {
        todo!()
    }
}
