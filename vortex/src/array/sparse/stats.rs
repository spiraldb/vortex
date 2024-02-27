use crate::array::sparse::SparseArray;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for SparseArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        todo!()
    }
}
