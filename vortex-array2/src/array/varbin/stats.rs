use std::cmp::Ordering;
use std::collections::HashMap;

use vortex::scalar::Scalar;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::accessor::ArrayAccessor;
use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::stats::{ArrayStatisticsCompute, Stat};

impl ArrayStatisticsCompute for VarBinArray<'_> {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<HashMap<Stat, Scalar>> {
        self.with_iterator(|iter| {
            let mut acc = VarBinAccumulator::default();
            for next_val in iter {
                acc.nullable_next(next_val)
            }
            acc.finish(self.dtype())
        })
    }
}

pub struct VarBinAccumulator<'a> {
    min: &'a [u8],
    max: &'a [u8],
    is_constant: bool,
    is_sorted: bool,
    is_strict_sorted: bool,
    last_value: &'a [u8],
    null_count: usize,
    runs: usize,
}

impl Default for VarBinAccumulator<'_> {
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

impl<'a> VarBinAccumulator<'a> {
    pub fn nullable_next(&mut self, val: Option<&'a [u8]>) {
        match val {
            None => self.null_count += 1,
            Some(v) => self.next(v),
        }
    }

    pub fn next(&mut self, val: &'a [u8]) {
        if val < self.min {
            self.min.clone_from(&val);
        } else if val > self.max {
            self.max.clone_from(&val);
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

    pub fn finish(&self, dtype: &DType) -> HashMap<Stat, Scalar> {
        HashMap::from([
            (Stat::Min, varbin_scalar(self.min.to_vec(), dtype)),
            (Stat::Max, varbin_scalar(self.max.to_vec(), dtype)),
            (Stat::RunCount, self.runs.into()),
            (Stat::IsSorted, self.is_sorted.into()),
            (Stat::IsStrictSorted, self.is_strict_sorted.into()),
            (Stat::IsConstant, self.is_constant.into()),
            (Stat::NullCount, self.null_count.into()),
        ])
    }
}

#[cfg(test)]
mod test {
    use vortex_schema::{DType, Nullability};

    use crate::array::varbin::{OwnedVarBinArray, VarBinArray};
    use crate::stats::{ArrayStatistics, Stat};

    fn array(dtype: DType) -> OwnedVarBinArray {
        VarBinArray::from_vec(
            vec!["hello world", "hello world this is a long string"],
            dtype,
        )
    }

    #[test]
    fn utf8_stats() {
        let arr = array(DType::Utf8(Nullability::NonNullable));
        assert_eq!(
            arr.statistics().compute_as::<String>(Stat::Min).unwrap(),
            String::from("hello world")
        );
        assert_eq!(
            arr.statistics().compute_as::<String>(Stat::Max).unwrap(),
            String::from("hello world this is a long string")
        );
        assert_eq!(
            arr.statistics()
                .compute_as::<usize>(Stat::RunCount)
                .unwrap(),
            2
        );
        assert!(!arr
            .statistics()
            .compute_as::<bool>(Stat::IsConstant)
            .unwrap());
        assert!(arr.statistics().compute_as::<bool>(Stat::IsSorted).unwrap());
    }

    #[test]
    fn binary_stats() {
        let arr = array(DType::Binary(Nullability::NonNullable));
        assert_eq!(
            arr.statistics().compute_as::<Vec<u8>>(Stat::Min).unwrap(),
            "hello world".as_bytes().to_vec()
        );
        assert_eq!(
            arr.statistics().compute_as::<Vec<u8>>(Stat::Max).unwrap(),
            "hello world this is a long string".as_bytes().to_vec()
        );
        assert_eq!(
            arr.statistics()
                .compute_as::<usize>(Stat::RunCount)
                .unwrap(),
            2
        );
        assert!(!arr
            .statistics()
            .compute_as::<bool>(Stat::IsConstant)
            .unwrap());
        assert!(arr.statistics().compute_as::<bool>(Stat::IsSorted).unwrap());
    }
}
