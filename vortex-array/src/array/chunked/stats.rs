use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::stats::{ArrayStatisticsCompute, Stat, StatsSet};
use crate::stats::ArrayStatistics;

impl ArrayStatisticsCompute for ChunkedArray<'_> {
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
            }).unwrap_or_default())
    }
}
