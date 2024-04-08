use std::collections::HashMap;

use vortex::stats::{Stat, StatsCompute, StatsSet};
use vortex_error::VortexResult;

use crate::zigzag::ZigZagArray;

impl StatsCompute for ZigZagArray {
    fn compute(&self, _stat: Stat) -> VortexResult<StatsSet> {
        // TODO(ngates): implement based on the encoded array
        Ok(StatsSet::from(HashMap::new()))
    }
}
