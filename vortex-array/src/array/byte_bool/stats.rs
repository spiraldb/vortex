use vortex_error::VortexResult;

use super::ByteBoolArray;
use crate::{
    stats::{ArrayStatisticsCompute, Stat, StatsSet},
    // validity::{ArrayValidity, LogicalValidity},
    ArrayTrait,
};

impl ArrayStatisticsCompute for ByteBoolArray {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        if self.is_empty() {
            return Ok(StatsSet::new());
        }

        // match self.logical_validity() {
        //     LogicalValidity::AllValid(_) => self.boolean_buffer().compute_statistics(stat),
        //     LogicalValidity::AllInvalid(v) => all_null_stats(v),
        //     LogicalValidity::Array(a) => NullableBools(
        //         &self.boolean_buffer(),
        //         &a.into_array().flatten_bool()?.boolean_buffer(),
        //     )
        //     .compute_statistics(stat),
        // }
        todo!()
    }
}
