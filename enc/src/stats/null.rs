use std::collections::HashMap;

use crate::array::null::NullArray;
use crate::array::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for NullArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        StatsSet::from(HashMap::from([
            (Stat::IsConstant, true.into()),
            (Stat::IsSorted, true.into()),
            (Stat::RunCount, 1.into()),
        ]))
    }
}
