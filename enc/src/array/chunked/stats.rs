use crate::array::chunked::ChunkedArray;
use crate::array::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for ChunkedArray {
    fn compute(&self, _stat: Stat) -> StatsSet {
        todo!()
    }
}
