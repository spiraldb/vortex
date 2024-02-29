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

use vortex::stats::{Stat, StatsCompute, StatsSet};

use crate::dict::DictArray;

impl StatsCompute for DictArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        let mut stats = StatsSet::new();

        if let Some(rc) = self.codes().stats().get_or_compute(&Stat::RunCount) {
            stats.set(Stat::RunCount, rc);
        }
        if let Some(min) = self.dict().stats().get_or_compute(&Stat::Min) {
            stats.set(Stat::Min, min);
        }
        if let Some(max) = self.dict().stats().get_or_compute(&Stat::Max) {
            stats.set(Stat::Max, max);
        }
        if let Some(is_constant) = self.codes().stats().get_or_compute(&Stat::IsConstant) {
            stats.set(Stat::IsConstant, is_constant);
        }
        if let Some(null_count) = self.codes().stats().get_or_compute(&Stat::NullCount) {
            stats.set(Stat::NullCount, null_count);
        }

        // if dictionary is sorted
        if self
            .dict()
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsSorted)
            .unwrap_or(false)
        {
            if let Some(codes_are_sorted) = self
                .codes()
                .stats()
                .get_or_compute_as::<bool>(&Stat::IsSorted)
            {
                stats.set(Stat::IsSorted, codes_are_sorted.into());
            }

            if let Some(codes_are_strict_sorted) = self
                .codes()
                .stats()
                .get_or_compute_as::<bool>(&Stat::IsStrictSorted)
            {
                stats.set(Stat::IsStrictSorted, codes_are_strict_sorted.into());
            }
        }

        stats
    }
}
