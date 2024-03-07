use std::cmp::Ordering;
use std::collections::HashMap;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::array::Array;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for VarBinArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        self.iter_primitive()
            .map(|prim_iter| {
                let mut acc = VarBinAccumulator::<&[u8]>::default();
                for next_val in prim_iter {
                    acc.nullable_next(next_val);
                }
                Ok(acc.finish(self.dtype()))
            })
            .unwrap_or_else(|_| {
                let mut acc = VarBinAccumulator::<Vec<u8>>::default();
                for next_val in self.iter() {
                    acc.nullable_next(next_val);
                }
                Ok(acc.finish(self.dtype()))
            })
    }
}

pub struct VarBinAccumulator<T> {
    min: T,
    max: T,
    is_constant: bool,
    is_sorted: bool,
    is_strict_sorted: bool,
    last_value: T,
    null_count: usize,
    runs: usize,
}

impl Default for VarBinAccumulator<Vec<u8>> {
    fn default() -> Self {
        Self {
            min: vec![0xFF],
            max: vec![0x00],
            is_constant: true,
            is_sorted: true,
            is_strict_sorted: true,
            last_value: vec![0x00],
            runs: 0,
            null_count: 0,
        }
    }
}

impl VarBinAccumulator<Vec<u8>> {
    pub fn nullable_next(&mut self, val: Option<Vec<u8>>) {
        match val {
            None => self.null_count += 1,
            Some(v) => self.next(v),
        }
    }

    pub fn next(&mut self, val: Vec<u8>) {
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
}

impl<'a> Default for VarBinAccumulator<&'a [u8]> {
    fn default() -> Self {
        Self {
            min: &[0xFF],
            max: &[0x00],
            is_constant: true,
            is_sorted: true,
            is_strict_sorted: true,
            last_value: &[0x00],
            runs: 0,
            null_count: 0,
        }
    }
}

impl<'a> VarBinAccumulator<&'a [u8]> {
    pub fn nullable_next(&mut self, val: Option<&'a [u8]>) {
        match val {
            None => self.null_count += 1,
            Some(v) => self.next(v),
        }
    }

    pub fn next(&mut self, val: &'a [u8]) {
        if val < self.min {
            self.min = val;
        } else if val > self.max {
            self.max = val;
        }

        match val.cmp(self.last_value) {
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
}

impl<T: AsRef<[u8]>> VarBinAccumulator<T> {
    pub fn finish(&self, dtype: &DType) -> StatsSet {
        StatsSet::from(HashMap::from([
            (Stat::Min, varbin_scalar(self.min.as_ref().to_vec(), dtype)),
            (Stat::Max, varbin_scalar(self.max.as_ref().to_vec(), dtype)),
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
