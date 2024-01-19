use crate::array::stats::{Stat, StatsCompute, StatsSet};
use crate::array::varbinview::VarBinViewArray;

impl StatsCompute for VarBinViewArray {
    fn compute(&self, _stat: Stat) -> StatsSet {
        todo!()
    }
}
