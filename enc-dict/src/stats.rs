use crate::dict::DictArray;
use enc::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for DictArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        todo!()
    }
}
