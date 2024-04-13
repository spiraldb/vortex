use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::array::Array;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for VarBinArray {
    fn compute(&self, _stat: Stat) -> VortexResult<StatsSet> {
        if self.is_empty() {
            return Ok(StatsSet::new());
        }

        let mut acc = VarBinAccumulator::new();
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
        Ok(acc.finish(self.len(), self.dtype()))
    }
}

#[derive(Debug, Default)]
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

impl<'a> VarBinAccumulator<'a> {
    pub fn new() -> Self {
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

    pub fn finish(&self, len: usize, dtype: &DType) -> StatsSet {
        let mut stats = StatsSet::from(HashMap::from([
            (Stat::RunCount, self.runs.into()),
            (Stat::IsSorted, self.is_sorted.into()),
            (Stat::IsStrictSorted, self.is_strict_sorted.into()),
            (Stat::IsConstant, self.is_constant.into()),
            (Stat::NullCount, self.null_count.into()),
        ]));
        if self.null_count < len {
            stats.set(Stat::Min, varbin_scalar(self.min.to_vec(), dtype));
            stats.set(Stat::Max, varbin_scalar(self.max.to_vec(), dtype));
        }
        stats
    }
}

#[cfg(test)]
mod test {
    use vortex_schema::{DType, Nullability};

    use crate::array::varbin::VarBinArray;
    use crate::stats::{ArrayStatistics, Stat};

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
            arr.statistics().compute_min::<String>().unwrap(),
            "hello world".to_owned()
        );
        assert_eq!(
            arr.statistics().compute_max::<String>().unwrap(),
            "hello world this is a long string".to_owned()
        );
        assert_eq!(arr.statistics().compute_run_count().unwrap(), 2);
        assert!(!arr.statistics().compute_is_constant().unwrap());
        assert!(arr.statistics().compute_is_sorted().unwrap());
    }

    #[test]
    fn binary_stats() {
        let arr = array(DType::Binary(Nullability::NonNullable));
        assert_eq!(
            arr.statistics().compute_min::<Vec<u8>>().unwrap(),
            "hello world".as_bytes().to_vec()
        );
        assert_eq!(
            arr.statistics().compute_max::<Vec<u8>>().unwrap(),
            "hello world this is a long string".as_bytes().to_vec()
        );
        assert_eq!(arr.statistics().compute_run_count().unwrap(), 2);
        assert!(!arr.statistics().compute_is_constant().unwrap());
        assert!(arr.statistics().compute_is_sorted().unwrap());
    }

    #[test]
    fn some_nulls() {
        let array = VarBinArray::from_iter(
            vec![
                Some("hello world"),
                None,
                Some("hello world this is a long string"),
                None,
            ],
            DType::Utf8(Nullability::Nullable),
        );
        assert_eq!(
            array.statistics().compute_min::<String>().unwrap(),
            "hello world".to_owned()
        );
        assert_eq!(
            array.statistics().compute_max::<String>().unwrap(),
            "hello world this is a long string".to_owned()
        );
    }

    #[test]
    fn all_nulls() {
        let array = VarBinArray::from_iter(
            vec![Option::<&str>::None, None, None],
            DType::Utf8(Nullability::Nullable),
        );
        assert!(array.statistics().get(Stat::Min).is_none());
        assert!(array.statistics().get(Stat::Max).is_none());
    }
}
