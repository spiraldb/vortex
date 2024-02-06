use crate::array::nullable::NullableArray;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for NullableArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        todo!()
    }
}
