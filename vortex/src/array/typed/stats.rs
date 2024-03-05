use crate::array::typed::TypedArray;
use crate::error::VortexResult;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for &TypedArray {
    fn compute(self, _stat: &Stat) -> VortexResult<StatsSet> {
        todo!()
    }
}
