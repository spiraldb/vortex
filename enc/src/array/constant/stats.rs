use std::collections::HashMap;

use crate::array::constant::ConstantArray;
use crate::array::Array;
use crate::dtype::{DType, Nullability};
use crate::scalar::{BoolScalar, PScalar, Scalar};
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for ConstantArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        let mut m = HashMap::from([
            (Stat::Max, dyn_clone::clone_box(self.value())),
            (Stat::Min, dyn_clone::clone_box(self.value())),
            (Stat::IsConstant, true.into()),
            (Stat::IsSorted, true.into()),
            (Stat::RunCount, 1.into()),
        ]);

        if matches!(self.dtype(), &DType::Bool(Nullability::NonNullable)) {
            m.insert(
                Stat::TrueCount,
                PScalar::U64(
                    self.len() as u64
                        * self
                            .value()
                            .as_any()
                            .downcast_ref::<BoolScalar>()
                            .unwrap()
                            .value() as u64,
                )
                .boxed(),
            );
        }
        StatsSet::from(m)
    }
}
