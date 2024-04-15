use std::collections::HashMap;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::constant::ConstantArray;
use crate::scalar::Scalar;
use crate::stats::{ArrayStatisticsCompute, Stat};

impl ArrayStatisticsCompute for ConstantArray<'_> {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<HashMap<Stat, Scalar>> {
        if matches!(self.dtype(), &DType::Bool(_)) {
            let Scalar::Bool(b) = self.scalar() else {
                unreachable!("Got bool dtype without bool scalar")
            };
            return Ok([(
                Stat::TrueCount,
                (self.len() as u64 * b.value().cloned().map(|v| v as u64).unwrap_or(0)).into(),
            )]
            .into());
        }
        Ok(HashMap::default())
    }
}
