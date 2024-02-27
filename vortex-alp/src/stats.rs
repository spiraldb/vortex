use std::collections::HashMap;

use crate::ALPArray;
use vortex::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for ALPArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        // TODO(ngates): implement based on the encoded array
        StatsSet::from(HashMap::new())
    }
}
