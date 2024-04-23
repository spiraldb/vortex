use std::collections::HashMap;

use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::scalar::Scalar;
use crate::stats::{ArrayStatisticsCompute, Stat};

impl ArrayStatisticsCompute for ChunkedArray<'_> {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<HashMap<Stat, Scalar>> {
        todo!()
    }
}
