use std::collections::HashMap;

use arrow::buffer::ScalarBuffer;
use half::f16;
use polars_core::prelude::{Series, SortOptions};
use polars_ops::prelude::SeriesMethods;

use crate::array::primitive::PrimitiveArray;
use crate::array::stats::{Stat, StatsCompute, StatsSet};
use crate::array::ArrayEncoding;
use crate::polars::IntoPolarsSeries;
use crate::scalar::ListScalar;
use crate::scalar::Scalar;
use crate::types::{match_each_integral_float_ptype, PType};

impl StatsCompute for PrimitiveArray {
    fn compute(&self, _stat: Stat) -> StatsSet {
        let s: Series = self.iter_arrow().into_polars();
        let mut m = HashMap::new();
        let is_sorted = s.is_sorted(SortOptions::default()).unwrap();
        m.insert(Stat::IsSorted, is_sorted.into());

        match_each_integral_float_ptype!(self.ptype, |$I| {
            let mins: $I = s.min().unwrap().unwrap();
            m.insert(Stat::Min, mins.into());
            let maxs: $I = s.max().unwrap().unwrap();
            m.insert(Stat::Max, maxs.into());
            m.insert(Stat::IsConstant, (mins == maxs).into());

            let bitwidth = std::mem::size_of::<u64>() * 8;
            let mut bit_widths: Vec<u64> = vec![0; bitwidth + 1];
            let typed_buf = ScalarBuffer::<$I>::from(self.buffer.clone());
            let mut last_val: $I = typed_buf[0];
            let mut run_count: usize = 0;
            for v in &typed_buf {
                bit_widths[bitwidth - v.leading_zeros() as usize] += 1;
                if (last_val != *v) {
                    run_count += 1;
                }
                last_val = *v;
            }
            let bit_widths_s: ListScalar = bit_widths.into();
            m.insert(Stat::BitWidthFreq, bit_widths_s.boxed());
            run_count += 1;
            m.insert(Stat::RunCount, run_count.into());
        },
            |$F| {
            let mins: $F = s.min().unwrap().unwrap();
            m.insert(Stat::Min, mins.into());
            let maxs: $F = s.max().unwrap().unwrap();
            m.insert(Stat::Max, maxs.into());
            m.insert(Stat::IsConstant, (mins == maxs).into());
            let typed_buf = ScalarBuffer::<$F>::from(self.buffer.clone());
            let mut last_val: $F = typed_buf[0];
            let mut run_count: usize = 0;
            for v in &typed_buf {
                if (last_val != *v) {
                    run_count += 1;
                }
                last_val = *v;
            }
            run_count += 1;
            m.insert(Stat::RunCount, run_count.into());

        });
        m
    }
}

#[cfg(test)]
mod test {
    use crate::error::EncResult;

    use super::*;

    #[test]
    fn stats() -> EncResult<()> {
        let arr = PrimitiveArray::from_vec(vec![1, 2, 3, 4, 5]);
        let min: i32 = arr.stats().get_or_compute_as(Stat::Min)?.unwrap();
        let max: i32 = arr.stats().get_or_compute_as(Stat::Max)?.unwrap();
        let is_sorted: bool = arr.stats().get_or_compute_as(Stat::IsSorted)?.unwrap();
        let is_constant: bool = arr.stats().get_or_compute_as(Stat::IsConstant)?.unwrap();
        let bit_width_freq: Vec<u64> = arr.stats().get_or_compute_as(Stat::BitWidthFreq)?.unwrap();
        let run_count: u64 = arr.stats().get_or_compute_as(Stat::RunCount)?.unwrap();
        assert_eq!(min, 1);
        assert_eq!(max, 5);
        assert!(is_sorted);
        assert!(!is_constant);
        assert_eq!(
            bit_width_freq,
            vec![
                0u64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 1, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        assert_eq!(run_count, 5);
        Ok(())
    }
}
