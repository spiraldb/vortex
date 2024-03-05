use std::collections::HashMap;

use crate::array::constant::ConstantArray;
use crate::array::Array;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::scalar::{PScalar, PrimitiveScalar, Scalar};
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for ConstantArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        let mut m = HashMap::from([
            (Stat::Max, self.scalar().clone()),
            (Stat::Min, self.scalar().clone()),
            (Stat::IsConstant, true.into()),
            (Stat::IsSorted, true.into()),
            (Stat::RunCount, 1.into()),
        ]);

        if matches!(self.dtype(), &DType::Bool(_)) {
            let Scalar::Bool(b) = self.scalar() else {
                unreachable!("Got bool dtype without bool scalar")
            };
            m.insert(
                Stat::TrueCount,
                PrimitiveScalar::some(PScalar::U64(
                    self.len() as u64 * b.value().map(|v| v as u64).unwrap_or(0),
                ))
                .into(),
            );
        }

        Ok(StatsSet::from(m))
    }
}
