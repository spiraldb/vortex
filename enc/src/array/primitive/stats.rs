use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;

use arrow::datatypes::ArrowNativeType;
use half::f16;
use num_traits::{NumCast, PrimInt};

use crate::array::primitive::PrimitiveArray;
use crate::array::Array;
use crate::ptype::{match_each_native_ptype, PType};
use crate::scalar::ListScalarValues;
use crate::scalar::Scalar;
use crate::stats::{Stat, StatsCompute, StatsSet};

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

fn integer_stats<T: ArrowNativeType + NumCast + PrimInt + Hash>(array: &PrimitiveArray) -> StatsSet
where
    Box<dyn Scalar>: From<T>,
{
    let bitwidth = std::mem::size_of::<u64>() * 8;
    let mut bit_widths: Vec<u64> = vec![0; bitwidth + 1];

    let typed_buf: &[T] = array.buffer().typed_data();
    // TODO(ngates): bail out on empty stats

    let mut unique = HashSet::new();
    let mut is_unique = true;
    let mut is_sorted = true;
    let mut min = typed_buf[0];
    let mut max = typed_buf[0];
    let mut last_val = typed_buf[0];
    let mut run_count: usize = 0;

    for v in typed_buf {
        bit_widths[bitwidth - v.leading_zeros() as usize] += 1;
        if last_val != *v {
            if *v < last_val {
                is_sorted = false;
            }
            run_count += 1;
        }
        if *v < min {
            min = *v;
        } else if *v > max {
            max = *v;
        }
        if is_unique && !unique.insert(*v) {
            is_unique = false;
        }
        last_val = *v;
    }
    run_count += 1;

    StatsSet::from(HashMap::from([
        (Stat::Min, min.into()),
        (Stat::Max, max.into()),
        (Stat::IsConstant, (min == max).into()),
        (Stat::BitWidthFreq, ListScalarValues(bit_widths).into()),
        (Stat::IsSorted, is_sorted.into()),
        (Stat::IsUnique, is_unique.into()),
        (Stat::RunCount, run_count.into()),
    ]))
}

fn float_stats<T: ArrowNativeType + NumCast>(array: &PrimitiveArray) -> StatsSet
where
    Box<dyn Scalar>: From<T>,
{
    let typed_buf: &[T] = array.buffer().typed_data();
    // TODO: bail out on empty stats

    let mut min = typed_buf[0];
    let mut max = typed_buf[0];
    let mut last_val: T = typed_buf[0];
    let mut is_sorted = true;
    let mut run_count: usize = 0;
    for v in typed_buf {
        if last_val != *v {
            run_count += 1;
            if *v < last_val {
                is_sorted = false;
            }
        }
        if *v < min {
            min = *v;
        } else if *v > max {
            max = *v;
        }
        last_val = *v;
    }
    run_count += 1;

    StatsSet::from(HashMap::from([
        (Stat::Min, min.into()),
        (Stat::Max, max.into()),
        (Stat::IsConstant, (min == max).into()),
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
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ]
        );
        assert_eq!(run_count, 5);
    }

    #[test]
    fn stats_u8() {
        let arr = PrimitiveArray::from_vec::<u8>(vec![1, 2, 3, 4, 5]);
        let min: u8 = arr.stats().get_or_compute_as(&Stat::Min).unwrap();
        let max: u8 = arr.stats().get_or_compute_as(&Stat::Max).unwrap();
        assert_eq!(min, 1);
        assert_eq!(max, 5);
    }
}
