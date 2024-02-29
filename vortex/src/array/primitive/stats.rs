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
use std::marker::PhantomData;

use arrow::datatypes::ArrowNativeType;
use half::f16;
use num_traits::{NumCast, PrimInt};

use crate::array::primitive::PrimitiveArray;
use crate::ptype::match_each_native_ptype;
use crate::scalar::ListScalarVec;
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

fn integer_stats<T: ArrowNativeType + NumCast + PrimInt>(array: &PrimitiveArray) -> StatsSet
where
    Box<dyn Scalar>: From<T>,
{
    let typed_buf: &[T] = array.buffer().typed_data();
    // TODO(ngates): bail out on empty stats

    let bitwidth = std::mem::size_of::<T>() * 8;
    let mut bit_widths: Vec<u64> = vec![0; bitwidth + 1];
    bit_widths[bitwidth - typed_buf[0].leading_zeros() as usize] += 1;

    let mut is_sorted = true;
    let mut is_strict_sorted = true;
    let mut min = typed_buf[0];
    let mut max = typed_buf[0];
    let mut last_val = typed_buf[0];
    let mut run_count: usize = 0;

    for v in &typed_buf[1..] {
        bit_widths[bitwidth - v.leading_zeros() as usize] += 1;
        if last_val == *v {
            is_strict_sorted = false;
        } else {
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
        last_val = *v;
    }
    run_count += 1;

    StatsSet::from(HashMap::from([
        (Stat::Min, min.into()),
        (Stat::Max, max.into()),
        (Stat::IsConstant, (min == max).into()),
        (Stat::BitWidthFreq, ListScalarVec(bit_widths).into()),
        (Stat::IsSorted, is_sorted.into()),
        (Stat::IsStrictSorted, (is_sorted && is_strict_sorted).into()),
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
    for v in &typed_buf[1..] {
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
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Array;
    use crate::scalar::ListScalarVec;
    use crate::stats::Stat;

    #[test]
    fn stats() {
        let arr = PrimitiveArray::from_vec(vec![1, 2, 3, 4, 5]);
        let min: i32 = arr.stats().get_or_compute_as(&Stat::Min).unwrap();
        let max: i32 = arr.stats().get_or_compute_as(&Stat::Max).unwrap();
        let is_sorted: bool = arr.stats().get_or_compute_as(&Stat::IsSorted).unwrap();
        let is_strict_sorted: bool = arr
            .stats()
            .get_or_compute_as(&Stat::IsStrictSorted)
            .unwrap();
        let is_constant: bool = arr.stats().get_or_compute_as(&Stat::IsConstant).unwrap();
        let bit_width_freq: Vec<u64> = arr
            .stats()
            .get_or_compute_as::<ListScalarVec<u64>>(&Stat::BitWidthFreq)
            .unwrap()
            .0;
        let run_count: u64 = arr.stats().get_or_compute_as(&Stat::RunCount).unwrap();
        assert_eq!(min, 1);
        assert_eq!(max, 5);
        assert!(is_sorted);
        assert!(is_strict_sorted);
        assert!(!is_constant);
        assert_eq!(
            bit_width_freq,
            vec![
                0u64, 1, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0
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
