// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use crate::array::constant::ConstantArray;
use crate::array::Array;
use crate::dtype::{DType, Nullability};
use crate::scalar::{BoolScalar, PScalar, Scalar};
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for ConstantArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        let mut m = HashMap::from([
            (Stat::Max, dyn_clone::clone_box(self.scalar())),
            (Stat::Min, dyn_clone::clone_box(self.scalar())),
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
                            .scalar()
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
