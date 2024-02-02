use std::collections::HashMap;

use crate::array::zigzag::ZigZagArray;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for ZigZagArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        // TODO(ngates): implement based on the encoded array
        StatsSet::from(HashMap::new())
    }
}
