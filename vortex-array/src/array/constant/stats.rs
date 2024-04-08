use std::collections::HashMap;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::constant::ConstantArray;
use crate::array::Array;
use crate::scalar::Scalar;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for ConstantArray {
    fn compute(&self, _stat: Stat) -> VortexResult<StatsSet> {
        if matches!(self.dtype(), &DType::Bool(_)) {
            let Scalar::Bool(b) = self.scalar() else {
                unreachable!("Got bool dtype without bool scalar")
            };
            return Ok(StatsSet::from(HashMap::from([(
                Stat::TrueCount,
                (self.len() as u64 * b.value().cloned().map(|v| v as u64).unwrap_or(0)).into(),
            )])));
        }

        Ok(StatsSet::default())
    }
}
