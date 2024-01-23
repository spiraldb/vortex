use crate::array::stats::{Stat, StatsCompute, StatsSet};
use crate::array::typed::TypedArray;

impl StatsCompute for TypedArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        todo!()
    }
}
