use crate::array::typed::TypedArray;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for TypedArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        todo!()
    }
}
