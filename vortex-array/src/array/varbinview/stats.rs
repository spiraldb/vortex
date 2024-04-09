use vortex_error::VortexResult;

use crate::array::varbin::VarBinAccumulator;
use crate::array::varbinview::VarBinViewArray;
use crate::array::Array;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for VarBinViewArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        self.iter_primitive()
            .map(|prim_iter| {
                let mut acc = VarBinAccumulator::<&[u8]>::default();
                for next_val in prim_iter {
                    acc.nullable_next(next_val);
                }
                Ok(acc.finish(self.dtype()))
            })
            .unwrap_or_else(|_| {
                let mut acc = VarBinAccumulator::<Vec<u8>>::default();
                for next_val in self.iter() {
                    acc.nullable_next(next_val);
                }
                Ok(acc.finish(self.dtype()))
            })
    }
}
