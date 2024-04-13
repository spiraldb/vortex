use std::borrow::Cow;

use vortex_error::VortexResult;

use crate::array::varbin::compute_stats;
use crate::array::varbinview::VarBinViewArray;
use crate::array::Array;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for VarBinViewArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        if self.is_empty() {
            return Ok(StatsSet::new());
        }

        Ok(self
            .iter_primitive()
            .map(|prim_iter| compute_stats(&mut prim_iter.map(|s| s.map(Cow::from)), self.dtype()))
            .unwrap_or_else(|_| {
                compute_stats(&mut self.iter().map(|s| s.map(Cow::from)), self.dtype())
            }))
    }
}
