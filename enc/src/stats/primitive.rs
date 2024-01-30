use std::collections::HashMap;
use std::marker::PhantomData;

use arrow::datatypes::ArrowNativeType;
use half::f16;
use num_traits::{NumCast, PrimInt};
use polars_core::prelude::{Series, SortOptions};
use polars_ops::prelude::SeriesMethods;

use crate::array::primitive::PrimitiveArray;
use crate::array::stats::{Stat, StatsCompute, StatsSet};
use crate::array::Array;
use crate::polars::IntoPolarsSeries;
use crate::scalar::ListScalarValues;
use crate::scalar::Scalar;
use crate::types::{match_each_native_ptype, PType};

impl StatsCompute for PrimitiveArray {
    fn compute(&self, stat: &Stat) -> StatsSet {
        match_each_native_ptype!(self.ptype(), |$P| {
            WrappedPrimitive::<$P>::new(self).compute(stat)
        })
    }
}

struct WrappedPrimitive<'a, P>(&'a PrimitiveArray, PhantomData<P>);

impl<'a, P> WrappedPrimitive<'a, P> {
    pub fn new(array: &'a PrimitiveArray) -> Self {
        Self(array, PhantomData)
    }
}

macro_rules! integer_stats {
    ($T:ty) => {
        impl StatsCompute for WrappedPrimitive<'_, $T> {
            fn compute(&self, _stat: &Stat) -> StatsSet {
                integer_stats::<$T>(self.0)
            }
        }
    };
}

integer_stats!(i8);
integer_stats!(i16);
integer_stats!(i32);
integer_stats!(i64);
integer_stats!(u8);
integer_stats!(u16);
integer_stats!(u32);
integer_stats!(u64);

macro_rules! float_stats {
    ($T:ty) => {
        impl StatsCompute for WrappedPrimitive<'_, $T> {
            fn compute(&self, _stat: &Stat) -> StatsSet {
                float_stats::<$T>(self.0)
            }
        }
    };
}

float_stats!(f16);
float_stats!(f32);
float_stats!(f64);

fn integer_stats<T: ArrowNativeType + NumCast + PrimInt>(array: &PrimitiveArray) -> StatsSet
where
    Box<dyn Scalar>: From<T>,
{
    let s: Series = array.iter_arrow().into_polars();
    let is_sorted = s.is_sorted(SortOptions::default()).unwrap();
    let mins: T = s.min().unwrap().unwrap();
    let maxs: T = s.max().unwrap().unwrap();

    let bitwidth = std::mem::size_of::<u64>() * 8;
    let mut bit_widths: Vec<u64> = vec![0; bitwidth + 1];

    let typed_buf: &[T] = array.buffer().typed_data();
    let mut last_val = typed_buf[0];
    let mut run_count: usize = 0;
    for v in typed_buf {
        bit_widths[bitwidth - v.leading_zeros() as usize] += 1;
        if last_val != *v {
            run_count += 1;
        }
        last_val = *v;
    }
    run_count += 1;

    StatsSet::from(HashMap::from([
        (Stat::Min, mins.into()),
        (Stat::Max, maxs.into()),
        (Stat::IsConstant, (mins == maxs).into()),
        (Stat::BitWidthFreq, ListScalarValues(bit_widths).into()),
        (Stat::IsSorted, is_sorted.into()),
        (Stat::RunCount, run_count.into()),
    ]))
}

fn float_stats<T: ArrowNativeType + NumCast>(array: &PrimitiveArray) -> StatsSet
where
    Box<dyn Scalar>: From<T>,
{
    let s: Series = array.iter_arrow().into_polars();
    let is_sorted = s.is_sorted(SortOptions::default()).unwrap();

    let mins: T = s.min().unwrap().unwrap();
    let maxs: T = s.max().unwrap().unwrap();

    let typed_buf: &[T] = array.buffer().typed_data();
    let mut last_val: T = typed_buf[0];
    let mut run_count: usize = 0;
    for v in typed_buf {
        if last_val != *v {
            run_count += 1;
        }
        last_val = *v;
    }
    run_count += 1;

    StatsSet::from(HashMap::from([
        (Stat::Min, mins.into()),
        (Stat::Max, maxs.into()),
        (Stat::IsConstant, (mins == maxs).into()),
        (Stat::IsSorted, is_sorted.into()),
        (Stat::RunCount, run_count.into()),
    ]))
}

#[cfg(test)]
mod test {
    use crate::scalar::ListScalarValues;

    use super::*;

    #[test]
    fn stats() {
        let arr = PrimitiveArray::from_vec(vec![1, 2, 3, 4, 5]);
        let min: i32 = arr.stats().get_or_compute_as(&Stat::Min).unwrap();
        let max: i32 = arr.stats().get_or_compute_as(&Stat::Max).unwrap();
        let is_sorted: bool = arr.stats().get_or_compute_as(&Stat::IsSorted).unwrap();
        let is_constant: bool = arr.stats().get_or_compute_as(&Stat::IsConstant).unwrap();
        let bit_width_freq: Vec<u64> = arr
            .stats()
            .get_or_compute_as::<ListScalarValues<u64>>(&Stat::BitWidthFreq)
            .unwrap()
            .0;
        let run_count: u64 = arr.stats().get_or_compute_as(&Stat::RunCount).unwrap();
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
    }
}
