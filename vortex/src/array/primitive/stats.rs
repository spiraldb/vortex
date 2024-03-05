use arrow::buffer::BooleanBuffer;
use arrow::datatypes::ArrowNativeType;
use half::f16;
use num_traits::{NumCast, PrimInt};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::size_of;

use crate::array::primitive::PrimitiveArray;
use crate::compute::cast::cast_bool;
use crate::error::VortexResult;
use crate::ptype::NativePType;
use crate::scalar::{ListScalarVec, ScalarRef};
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for PrimitiveArray {
    fn compute(&self, stat: &Stat) -> VortexResult<StatsSet> {
        // let mut iter = self.typed_data::<i16>().iter().peekable();
        // match iter.peek() {
        //     // No values at all
        //     None => return Ok(StatsSet::default()),
        //     Some(first) => {
        //         StatsAccumulator::new(**first);
        //     }
        // }

        match self.validity() {
            None => self.typed_data::<u16>().compute(stat),
            Some(validity_array) => {
                let validity = cast_bool(validity_array)?;
                NullableValues(self.typed_data::<u16>(), validity.buffer()).compute(stat)
            }
        }
    }
}

impl<'a, T: NativePType> StatsCompute for &[T] {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        if self.is_empty() {
            return Ok(StatsSet::default());
        }
        let mut stats = StatsAccumulator::new(self[0]);
        self.iter().skip(1).for_each(|next| stats.next(*next));
        Ok(stats.into_set())
    }
}

struct NullableValues<'a, T: NativePType>(&'a [T], &'a BooleanBuffer);

impl<'a, T: NativePType> StatsCompute for NullableValues<'a, T> {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        let values = self.0;
        if values.is_empty() {
            return Ok(StatsSet::default());
        }

        let first = if self.1.value(0) {
            Some(values[0])
        } else {
            None
        };
        let mut stats = StatsAccumulator::new(first);

        values
            .iter()
            .zip(self.1.iter())
            .skip(1)
            .map(|(next, valid)| if valid { Some(*next) } else { None })
            .for_each(|next| stats.next(next));
        Ok(stats.into_set())
    }
}

trait BitWidth {
    fn bit_width(&self) -> usize;
}
//
// impl<T: PrimInt> BitWidth for PInt<T> {
//     fn bit_width(&self) -> usize {
//         (size_of::<T>() * 8) - self.leading_zeros() as usize
//     }
// }

impl<T: NativePType> BitWidth for T {
    fn bit_width(&self) -> usize {
        size_of::<T>() * 8
    }
}

impl<T: BitWidth> BitWidth for Option<T> {
    fn bit_width(&self) -> usize {
        match self {
            Some(v) => v.bit_width(),
            None => 0,
        }
    }
}

trait SupportsPrimitiveStats: Into<ScalarRef> + BitWidth + PartialEq + PartialOrd + Copy {}
impl<T> SupportsPrimitiveStats for T where T: NativePType {}
impl<T: NativePType> SupportsPrimitiveStats for Option<T> where Option<T>: Into<ScalarRef> {}

struct StatsAccumulator<T: SupportsPrimitiveStats> {
    prev: T,
    min: T,
    max: T,
    is_sorted: bool,
    is_strict_sorted: bool,
    is_constant: bool,
    run_count: usize,
    bit_widths: [usize; 65], // TODO(ngates): const exprs? (size_of::<T>() * 8) + 1,
}

impl<T: SupportsPrimitiveStats> StatsAccumulator<T> {
    fn new(first_value: T) -> Self {
        let mut stats = Self {
            prev: first_value.clone(),
            min: first_value.clone(),
            max: first_value.clone(),
            is_sorted: true,
            is_strict_sorted: true,
            is_constant: true,
            run_count: 1,
            bit_widths: [0; 65],
        };
        stats.bit_widths[first_value.bit_width()] += 1;
        stats
    }

    pub fn next(self: &mut Self, next: T) {
        self.bit_widths[next.bit_width()] += 1;

        if self.prev == next {
            self.is_strict_sorted = false;
        } else {
            if next < self.prev {
                self.is_sorted = false;
            }
            self.run_count += 1;
        }
        if next < self.min {
            self.min = next;
        } else if next > self.max {
            self.max = next;
        }
        self.prev = next;
    }

    pub fn into_set(self: Self) -> StatsSet {
        StatsSet::from(HashMap::from([
            (Stat::Min, self.min.into()),
            (Stat::Max, self.max.into()),
            (Stat::IsConstant, self.is_constant.into()),
            (
                Stat::BitWidthFreq,
                ListScalarVec(self.bit_widths.to_vec()).into(),
            ),
            (Stat::IsSorted, self.is_sorted.into()),
            (
                Stat::IsStrictSorted,
                (self.is_sorted && self.is_strict_sorted).into(),
            ),
            (Stat::RunCount, self.run_count.into()),
        ]))
    }
}

struct WrappedPrimitive<'a, P>(&'a PrimitiveArray, PhantomData<P>);

macro_rules! integer_stats {
    ($T:ty) => {
        impl StatsCompute for WrappedPrimitive<'_, $T> {
            fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
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
            fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
                float_stats::<$T>(self.0)
            }
        }
    };
}

float_stats!(f16);
float_stats!(f32);
float_stats!(f64);

fn integer_stats<T: ArrowNativeType + NumCast + PrimInt>(
    array: &PrimitiveArray,
) -> VortexResult<StatsSet>
where
    ScalarRef: From<T>,
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

    Ok(StatsSet::from(HashMap::from([
        (Stat::Min, min.into()),
        (Stat::Max, max.into()),
        (Stat::IsConstant, (min == max).into()),
        (Stat::BitWidthFreq, ListScalarVec(bit_widths).into()),
        (Stat::IsSorted, is_sorted.into()),
        (Stat::IsStrictSorted, (is_sorted && is_strict_sorted).into()),
        (Stat::RunCount, run_count.into()),
    ])))
}

fn float_stats<T: ArrowNativeType + NumCast>(array: &PrimitiveArray) -> VortexResult<StatsSet>
where
    ScalarRef: From<T>,
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

    Ok(StatsSet::from(HashMap::from([
        (Stat::Min, min.into()),
        (Stat::Max, max.into()),
        (Stat::IsConstant, (min == max).into()),
        (Stat::IsSorted, is_sorted.into()),
        (Stat::RunCount, run_count.into()),
    ])))
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
