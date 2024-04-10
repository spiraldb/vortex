use std::borrow::Cow;

use vortex_error::VortexResult;

use crate::array::varbin::VarBinAccumulator;
use crate::array::varbinview::VarBinViewArray;
use crate::array::Array;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for VarBinViewArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        let mut acc = VarBinAccumulator::default();
        self.iter_primitive()
            .map(|prim_iter| {
                for next_val in prim_iter {
                    acc.nullable_next(next_val.map(Cow::from));
                }
            })
            .unwrap_or_else(|_| {
                for next_val in self.iter() {
                    acc.nullable_next(next_val.map(Cow::from));
                }
            });
        Ok(acc.finish(self.dtype()))
    }
}
