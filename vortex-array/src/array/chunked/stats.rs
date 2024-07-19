use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::stats::{ArrayStatistics, ArrayStatisticsCompute, Stat, StatsSet};

impl ArrayStatisticsCompute for ChunkedArray {
    fn compute_statistics(&self, stat: Stat) -> VortexResult<StatsSet> {
        Ok(self
            .chunks()
            .map(|c| {
                let s = c.statistics();
                // HACK(robert): This will compute all stats, but we could just compute one
                s.compute(stat);
                s.to_set()
            })
            .reduce(|mut acc, x| {
                acc.merge(&x);
                acc
            })
            .unwrap_or_default())
    }
}
