use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::array::Array;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for VarBinArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        let mut acc = VarBinAccumulator::default();
        self.iter_primitive()
            .map(|prim_iter| {
                for next_val in prim_iter {
                    acc.nullable_next(next_val.map(Cow::from));
                }
            })
            .unwrap_or_else(|_| {
                for next_val in self.iter() {
                    acc.nullable_next(next_val.map(Cow::from));
                }
            });
        Ok(acc.finish(self.dtype()))
    }
}

pub struct VarBinAccumulator<'a> {
    min: Cow<'a, [u8]>,
    max: Cow<'a, [u8]>,
    is_constant: bool,
    is_sorted: bool,
    is_strict_sorted: bool,
    last_value: Cow<'a, [u8]>,
    null_count: usize,
    runs: usize,
}

impl Default for VarBinAccumulator<'_> {
    fn default() -> Self {
        Self {
            min: Cow::from(&[0xFF]),
            max: Cow::from(&[0x00]),
            is_constant: true,
            is_sorted: true,
            is_strict_sorted: true,
            last_value: Cow::from(&[0x00]),
            runs: 0,
            null_count: 0,
        }
    }
}

impl<'a> VarBinAccumulator<'a> {
    pub fn nullable_next(&mut self, val: Option<Cow<'a, [u8]>>) {
        match val {
            None => self.null_count += 1,
            Some(v) => self.next(v),
        }
    }

    pub fn next(&mut self, val: Cow<'a, [u8]>) {
        if val < self.min {
            self.min.clone_from(&val);
        } else if val > self.max {
            self.max.clone_from(&val);
        }

        match val.cmp(&self.last_value) {
            Ordering::Less => self.is_sorted = false,
            Ordering::Equal => {
                self.is_strict_sorted = false;
                return;
            }
            Ordering::Greater => {}
        }
        self.is_constant = false;
        self.last_value = val;
        self.runs += 1;
    }

    pub fn finish(&self, dtype: &DType) -> StatsSet {
        StatsSet::from(HashMap::from([
            (Stat::Min, varbin_scalar(self.min.to_vec(), dtype)),
            (Stat::Max, varbin_scalar(self.max.to_vec(), dtype)),
            (Stat::RunCount, self.runs.into()),
            (Stat::IsSorted, self.is_sorted.into()),
            (Stat::IsStrictSorted, self.is_strict_sorted.into()),
            (Stat::IsConstant, self.is_constant.into()),
            (Stat::NullCount, self.null_count.into()),
        ]))
    }
}

#[cfg(test)]
mod test {
    use vortex_schema::{DType, Nullability};

    use crate::array::varbin::VarBinArray;
    use crate::array::Array;
    use crate::stats::Stat;

    fn array(dtype: DType) -> VarBinArray {
        VarBinArray::from_vec(
            vec!["hello world", "hello world this is a long string"],
            dtype,
        )
    }

    #[test]
    fn utf8_stats() {
        let arr = array(DType::Utf8(Nullability::NonNullable));
        assert_eq!(
            arr.stats().get_or_compute_as::<String>(&Stat::Min).unwrap(),
            String::from("hello world")
        );
        assert_eq!(
            arr.stats().get_or_compute_as::<String>(&Stat::Max).unwrap(),
            String::from("hello world this is a long string")
        );
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<usize>(&Stat::RunCount)
                .unwrap(),
            2
        );
        assert!(!arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsConstant)
            .unwrap());
        assert!(arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsSorted)
            .unwrap());
    }

    #[test]
    fn binary_stats() {
        let arr = array(DType::Binary(Nullability::NonNullable));
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<Vec<u8>>(&Stat::Min)
                .unwrap(),
            "hello world".as_bytes().to_vec()
        );
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<Vec<u8>>(&Stat::Max)
                .unwrap(),
            "hello world this is a long string".as_bytes().to_vec()
        );
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<usize>(&Stat::RunCount)
                .unwrap(),
            2
        );
        assert!(!arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsConstant)
            .unwrap());
        assert!(arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsSorted)
            .unwrap());
    }
}
