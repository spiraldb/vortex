use std::collections::HashMap;

use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::accessor::ArrayAccessor;
use crate::array::varbin::compute_stats;
use crate::array::varbinview::VarBinViewArray;
use crate::stats::{ArrayStatisticsCompute, Stat};
use crate::{ArrayDType, ArrayTrait};

impl ArrayStatisticsCompute for VarBinViewArray<'_> {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<HashMap<Stat, Scalar>> {
        if self.is_empty() {
            return Ok(HashMap::new());
        }
        self.with_iterator(|iter| compute_stats(iter, self.dtype()))
    }
}
