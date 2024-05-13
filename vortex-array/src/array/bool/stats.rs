use std::collections::HashMap;

use arrow_buffer::BooleanBuffer;
use vortex_dtype::{DType, Nullability};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::bool::BoolArray;
use crate::stats::{ArrayStatisticsCompute, Stat, StatsSet};
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::{ArrayTrait, IntoArray};

impl ArrayStatisticsCompute for BoolArray {
    fn compute_statistics(&self, stat: Stat) -> VortexResult<StatsSet> {
        if self.is_empty() {
            return Ok(StatsSet::new());
        }

        match self.logical_validity() {
            LogicalValidity::AllValid(_) => self.boolean_buffer().compute_statistics(stat),
            LogicalValidity::AllInvalid(v) => all_null_stats(v),
            LogicalValidity::Array(a) => NullableBools(
                &self.boolean_buffer(),
                &a.into_array().flatten_bool()?.boolean_buffer(),
            )
            .compute_statistics(stat),
        }
    }
}

fn all_null_stats(len: usize) -> VortexResult<StatsSet> {
    Ok(StatsSet::from(HashMap::from([
        (Stat::Min, Scalar::null(DType::Bool(Nullability::Nullable))),
        (Stat::Max, Scalar::null(DType::Bool(Nullability::Nullable))),
        (Stat::IsConstant, true.into()),
        (Stat::IsSorted, true.into()),
        (Stat::IsStrictSorted, (len < 2).into()),
        (Stat::RunCount, 1.into()),
        (Stat::NullCount, len.into()),
    ])))
}

struct NullableBools<'a>(&'a BooleanBuffer, &'a BooleanBuffer);

impl ArrayStatisticsCompute for NullableBools<'_> {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        let first_non_null_idx = self
            .1
            .iter()
            .enumerate()
            .skip_while(|(_, valid)| !*valid)
            .map(|(idx, _)| idx)
            .next()
            .expect("Must be at least one non-null value");

        let mut stats = BoolStatsAccumulator::new_with_leading_nulls(
            self.0.value(first_non_null_idx),
            first_non_null_idx,
        );

        self.0
            .iter()
            .zip(self.1.iter())
            .skip(first_non_null_idx + 1)
            .map(|(next, valid)| valid.then_some(next))
            .for_each(|next| stats.nullable_next(next));
        Ok(stats.into_map(self.0.len()))
    }
}

impl ArrayStatisticsCompute for BooleanBuffer {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        let mut stats = BoolStatsAccumulator::new(self.value(0));
        self.iter().skip(1).for_each(|next| stats.next(next));
        Ok(stats.into_map(self.len()))
    }
}

struct BoolStatsAccumulator {
    prev: bool,
    is_sorted: bool,
    run_count: usize,
    null_count: usize,
    true_count: usize,
}

impl BoolStatsAccumulator {
    fn new(first_value: bool) -> Self {
        Self {
            prev: first_value,
            is_sorted: true,
            run_count: 1,
            null_count: 0,
            true_count: if first_value { 1 } else { 0 },
        }
    }

    fn new_with_leading_nulls(first_value: bool, leading_null_count: usize) -> Self {
        let mut stats = Self::new(first_value);
        stats.null_count += leading_null_count;
        stats
    }

    pub fn nullable_next(&mut self, next: Option<bool>) {
        match next {
            Some(n) => self.next(n),
            None => {
                self.null_count += 1;
            }
        }
    }

    pub fn next(&mut self, next: bool) {
        if next {
            self.true_count += 1
        }
        if !next & self.prev {
            self.is_sorted = false;
        }
        if next != self.prev {
            self.run_count += 1;
            self.prev = next;
        }
    }

    pub fn into_map(self, len: usize) -> StatsSet {
        StatsSet::from(HashMap::from([
            (Stat::Min, (self.true_count == len).into()),
            (Stat::Max, (self.true_count > 0).into()),
            (
                Stat::IsConstant,
                (self.true_count == len || self.true_count == 0).into(),
            ),
            (Stat::IsSorted, self.is_sorted.into()),
            (
                Stat::IsStrictSorted,
                (self.is_sorted && (len < 2 || (len == 2 && self.true_count == 1))).into(),
            ),
            (Stat::RunCount, self.run_count.into()),
            (Stat::TrueCount, self.true_count.into()),
        ]))
    }
}
