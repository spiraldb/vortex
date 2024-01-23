use std::cmp::Ordering;
use std::collections::HashMap;

use crate::array::stats::{Stat, StatsCompute, StatsSet};
use crate::array::varbin::VarBinArray;
use crate::array::ArrayEncoding;
use crate::scalar::{BinaryScalar, Utf8Scalar};
use crate::types::DType;

//TODO(robert): This could be better if we could get a view over underlying array. Taking scalars leads to unnecessary copies
macro_rules! varbin_stats {
    ($accessor:expr, $arr:expr) => {{
        let mut min = ($accessor)($arr, 0);
        let mut max = min.clone();
        let mut is_constant = true;
        let mut is_sorted = true;
        let mut last_value = min.clone();
        let mut runs: usize = 0;
        for i in 1..$arr.len() {
            let next_val = ($accessor)($arr, i);
            if next_val < min {
                min = next_val.clone();
            }
            if next_val > max {
                max = next_val.clone();
            }
            let cmp = next_val.cmp(&last_value);
            match cmp {
                Ordering::Less => is_sorted = false,
                Ordering::Equal => continue,
                Ordering::Greater => {}
            }
            is_constant = false;
            last_value = next_val;
            runs += 1;
        }
        runs += 1;

        StatsSet::from(HashMap::from([
            (Stat::Min, min.into()),
            (Stat::Max, max.into()),
            (Stat::RunCount, runs.into()),
            (Stat::IsSorted, is_sorted.into()),
            (Stat::IsConstant, is_constant.into()),
        ]))
    }};
}

pub(crate) use varbin_stats;

impl StatsCompute for VarBinArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        if self.len() == 0 {
            return StatsSet::new();
        }

        match self.dtype {
            DType::Utf8 => varbin_stats!(string_at, self),
            DType::Binary => varbin_stats!(binary_at, self),
            _ => panic!("Unexpected array dtype"),
        }
    }
}

fn string_at(arr: &VarBinArray, index: usize) -> String {
    arr.scalar_at(index)
        .unwrap()
        .into_any()
        .downcast::<Utf8Scalar>()
        .unwrap()
        .value()
        .to_string()
}

fn binary_at(arr: &VarBinArray, index: usize) -> Vec<u8> {
    arr.scalar_at(index)
        .unwrap()
        .into_any()
        .downcast::<BinaryScalar>()
        .unwrap()
        .value()
        .clone()
}

#[cfg(test)]
mod test {
    use crate::array::primitive::PrimitiveArray;
    use crate::array::stats::Stat;
    use crate::array::varbin::VarBinArray;
    use crate::array::ArrayEncoding;
    use crate::types::DType;

    fn array(dtype: DType) -> VarBinArray {
        let values = PrimitiveArray::from_vec(
            "hello worldhello world this is a long string"
                .as_bytes()
                .to_vec(),
        );
        let offsets = PrimitiveArray::from_vec(vec![0, 11, 44]);

        VarBinArray::new(Box::new(offsets.into()), Box::new(values.into()), dtype)
    }

    #[test]
    fn utf8_stats() {
        let arr = array(DType::Utf8);
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<String>(&Stat::Min)
                .unwrap()
                .unwrap(),
            String::from("hello world")
        );
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<String>(&Stat::Max)
                .unwrap()
                .unwrap(),
            String::from("hello world this is a long string")
        );
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<usize>(&Stat::RunCount)
                .unwrap()
                .unwrap(),
            2
        );
        assert!(!arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsConstant)
            .unwrap()
            .unwrap());
        assert!(arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsSorted)
            .unwrap()
            .unwrap());
    }

    #[test]
    fn binary_stats() {
        let arr = array(DType::Binary);
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<Vec<u8>>(&Stat::Min)
                .unwrap()
                .unwrap(),
            "hello world".as_bytes().to_vec()
        );
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<Vec<u8>>(&Stat::Max)
                .unwrap()
                .unwrap(),
            "hello world this is a long string".as_bytes().to_vec()
        );
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<usize>(&Stat::RunCount)
                .unwrap()
                .unwrap(),
            2
        );
        assert!(!arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsConstant)
            .unwrap()
            .unwrap());
        assert!(arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsSorted)
            .unwrap()
            .unwrap());
    }
}
