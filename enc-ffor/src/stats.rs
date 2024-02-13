use std::collections::HashMap;

use crate::FFORArray;
use enc::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for FFORArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        // TODO(ngates): implement based on the encoded array
        StatsSet::from(HashMap::new())
    }
}
