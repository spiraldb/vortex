use std::cmp::Ordering;
use std::collections::HashMap;

use crate::array::stats::{Stat, StatsCompute, StatsSet};
use crate::array::ArrayEncoding;
use crate::error::EncResult;
use crate::types::DType;

pub trait BinaryArray {
    fn bytes_at(&self, index: usize) -> EncResult<Vec<u8>>;
}

impl<T> StatsCompute for T
where
    T: BinaryArray + ArrayEncoding,
{
    fn compute(&self, _stat: &Stat) -> StatsSet {
        let mut min = vec![0xFF];
        let mut max = vec![0x00];
        let mut is_constant = true;
        let mut is_sorted = true;
        let mut last_value = vec![0x00];
        let mut runs: usize = 0;
        for i in 0..self.len() {
            let next_val = self.bytes_at(i).unwrap();
            if next_val < min {
                min = next_val.clone();
            }
            if next_val > max {
                max = next_val.clone();
            }
            match next_val.cmp(&last_value) {
                Ordering::Less => is_sorted = false,
                Ordering::Equal => continue,
                Ordering::Greater => {}
            }
            is_constant = false;
            last_value = next_val;
            runs += 1;
        }

        StatsSet::from(HashMap::from([
            (
                Stat::Min,
                if matches!(self.dtype(), DType::Utf8) {
                    unsafe { String::from_utf8_unchecked(min.to_vec()) }.into()
                } else {
                    min.into()
                },
            ),
            (
                Stat::Max,
                if matches!(self.dtype(), DType::Utf8) {
                    unsafe { String::from_utf8_unchecked(max.to_vec()) }.into()
                } else {
                    max.into()
                },
            ),
            (Stat::RunCount, runs.into()),
            (Stat::IsSorted, is_sorted.into()),
            (Stat::IsConstant, is_constant.into()),
        ]))
    }
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
