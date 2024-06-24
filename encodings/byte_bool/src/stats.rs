use std::collections::HashMap;

use vortex::{
    stats::{ArrayStatisticsCompute, Stat, StatsSet},
    validity::{ArrayValidity, LogicalValidity},
    ArrayDType, ArrayTrait, AsArray,
};
use vortex_error::VortexResult;

use super::ByteBoolArray;

impl ArrayStatisticsCompute for ByteBoolArray {
    fn compute_statistics(&self, stat: Stat) -> VortexResult<StatsSet> {
        if self.is_empty() {
            return Ok(StatsSet::new());
        }

        match self.logical_validity() {
            LogicalValidity::AllValid(len) => Ok(all_true_bool_stats(len)),
            LogicalValidity::AllInvalid(len) => Ok(StatsSet::nulls(len, self.dtype())),
            LogicalValidity::Array(_) => {
                let bools = self.as_array_ref().clone().flatten_bool()?;
                bools.compute_statistics(stat)
            }
        }
    }
}

fn all_true_bool_stats(len: usize) -> StatsSet {
    let stats = HashMap::from([
        (Stat::Min, true.into()),
        (Stat::Min, true.into()),
        (Stat::IsConstant, true.into()),
        (Stat::IsSorted, true.into()),
        (Stat::IsStrictSorted, (len < 2).into()),
        (Stat::RunCount, 1.into()),
        (Stat::NullCount, 0.into()),
    ]);

    StatsSet::from(stats)
}
