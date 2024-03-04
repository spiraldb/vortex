use arrow::buffer::NullBuffer;
use std::collections::HashMap;
use std::marker::PhantomData;

use crate::array::Array;
use arrow::datatypes::ArrowNativeType;
use half::f16;
use num_traits::{NumCast, PrimInt};

use crate::array::primitive::PrimitiveArray;
use crate::compute::cast::cast_bool;
use crate::error::VortexResult;
use crate::ptype::{match_each_native_ptype, NativePType};
use crate::scalar::ListScalarVec;
use crate::scalar::Scalar;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for PrimitiveArray {
    fn compute(&self, stat: &Stat) -> VortexResult<StatsSet> {
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

fn integer_stats<T: NativePType + ArrowNativeType + PrimInt>(
    array: &PrimitiveArray,
) -> VortexResult<StatsSet>
where
    Box<dyn Scalar>: From<T>,
{
    let typed_buf: &[T] = array.buffer().typed_data();

    // TODO(ngates): add optimized implementation for non-null;
    let validity = array.validity().map(cast_bool).transpose()?.map_or_else(
        || NullBuffer::new_valid(array.len()),
        |v| NullBuffer::from(v.buffer().clone()),
    );

    // TODO(ngates): bail out on empty stats
    let first = if validity.iter().next().unwrap() {
        Some(typed_buf[0])
    } else {
        None
    };

    let bitwidth = std::mem::size_of::<T>() * 8;
    let mut bit_widths: Vec<u64> = vec![0; bitwidth + 1];
    if let Some(v) = first {
        bit_widths[bitwidth - v.leading_zeros() as usize] += 1;
    } else {
        bit_widths[0] += 1;
    }

    let mut is_sorted = true;
    let mut is_strict_sorted = true;
    let mut min = first;
    let mut max = first;
    let mut last_val = first;
    let mut run_count: usize = 0;
    let mut null_count: usize = 0;

    typed_buf
        .iter()
        .zip(validity.iter())
        .skip(1)
        .map(|(v, is_valid)| if is_valid { Some(*v) } else { None })
        .for_each(|v| {
            if let Some(e) = v {
                bit_widths[bitwidth - e.leading_zeros() as usize] += 1;
            } else {
                null_count += 1;
                is_strict_sorted = false;
                bit_widths[0] += 1;
            }

            if last_val == v {
                is_strict_sorted = false;
            } else {
                if v < last_val {
                    is_sorted = false;
                }
                run_count += 1;
            }
            if v < min {
                min = v;
            } else if v > max {
                max = v;
            }
            last_val = v;
        });
    run_count += 1;

    let mut stats = HashMap::from([
        (Stat::IsConstant, (min == max).into()),
        (Stat::BitWidthFreq, ListScalarVec(bit_widths).into()),
        (Stat::IsSorted, is_sorted.into()),
        (Stat::IsStrictSorted, (is_sorted && is_strict_sorted).into()),
        (Stat::RunCount, run_count.into()),
    ]);

    if array.validity().is_some() {
        stats.insert(Stat::Min, min.into());
        stats.insert(Stat::Max, max.into());
    } else {
        // We must ensure min/max == the dtype of the array by unwrapping the optional
        min.map(|m| stats.insert(Stat::Min, m.into()));
        max.map(|m| stats.insert(Stat::Max, m.into()));
    }

    Ok(StatsSet::from(stats))
}

fn float_stats<T: ArrowNativeType + NumCast>(array: &PrimitiveArray) -> VortexResult<StatsSet>
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
