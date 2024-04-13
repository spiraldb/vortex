use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::array::Array;
use crate::scalar::Scalar;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for VarBinArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        if self.is_empty() {
            return Ok(StatsSet::new());
        }

        Ok(self
            .iter_primitive()
            .map(|prim_iter| compute_stats(&mut prim_iter.map(|s| s.map(Cow::from)), self.dtype()))
            .unwrap_or_else(|_| {
                compute_stats(&mut self.iter().map(|s| s.map(Cow::from)), self.dtype())
            }))
    }
}

pub fn compute_stats(
    iter: &mut dyn Iterator<Item = Option<Cow<'_, [u8]>>>,
    dtype: &DType,
) -> StatsSet {
    let mut leading_nulls = Vec::new();
    let mut first_value: Option<Cow<'_, [u8]>> = None;
    for v in &mut *iter {
        if v.is_none() {
            leading_nulls.push(v);
        } else {
            first_value = v;
            break;
        }
    }

    if let Some(first_non_null) = first_value {
        let mut acc = VarBinAccumulator::new(first_non_null);
        iter.for_each(|n| acc.nullable_next(n));
        acc.n_nulls(leading_nulls.len());
        acc.finish(dtype)
    } else {
        all_null_stats(leading_nulls.len(), dtype)
    }
}

fn all_null_stats(len: usize, dtype: &DType) -> StatsSet {
    StatsSet::from(HashMap::from([
        (Stat::Min, Scalar::null(dtype)),
        (Stat::Max, Scalar::null(dtype)),
        (Stat::IsConstant, true.into()),
        (Stat::IsSorted, true.into()),
        (Stat::IsStrictSorted, false.into()),
        (Stat::RunCount, 1.into()),
        (Stat::NullCount, len.into()),
    ]))
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
    pub fn new(value: Cow<'a, [u8]>) -> Self {
        Self {
            min: value.clone(),
            max: value.clone(),
            is_constant: true,
            is_sorted: true,
            is_strict_sorted: true,
            last_value: value,
            runs: 1,
            null_count: 0,
        }
    }

    pub fn nullable_next(&mut self, val: Option<Cow<'a, [u8]>>) {
        match val {
            None => self.null_count += 1,
            Some(v) => self.next(v),
        }
    }

    pub fn n_nulls(&mut self, null_count: usize) {
        self.null_count += null_count;
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
    use crate::scalar::Utf8Scalar;
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
            "hello world".to_owned()
        );
        assert_eq!(
            arr.stats().get_or_compute_as::<String>(&Stat::Max).unwrap(),
            "hello world this is a long string".to_owned()
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
            array
                .stats()
                .get_or_compute_as::<String>(&Stat::Min)
                .unwrap(),
            "hello world".to_owned()
        );
        assert_eq!(
            array
                .stats()
                .get_or_compute_as::<String>(&Stat::Max)
                .unwrap(),
            "hello world this is a long string".to_owned()
        );
    }

    #[test]
    fn all_nulls() {
        let array = VarBinArray::from_iter(
            vec![Option::<&str>::None, None, None],
            DType::Utf8(Nullability::Nullable),
        );
        assert_eq!(
            array.stats().get_or_compute(&Stat::Min).unwrap(),
            Utf8Scalar::none().into()
        );
        assert_eq!(
            array.stats().get_or_compute(&Stat::Max).unwrap(),
            Utf8Scalar::none().into()
        );
    }
}
