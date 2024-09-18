use std::collections::HashMap;

use croaring::Bitmap;
use vortex::stats::{ArrayStatisticsCompute, Stat, StatsSet};
use vortex_error::{vortex_err, VortexResult};

use crate::RoaringBoolArray;

impl ArrayStatisticsCompute for RoaringBoolArray {
    fn compute_statistics(&self, stat: Stat) -> VortexResult<StatsSet> {
        if self.is_empty() {
            return Ok(StatsSet::new());
        }

        BitmapStats(self.bitmap(), self.len()).compute_statistics(stat)
    }
}

struct BitmapStats(Bitmap, usize);

impl ArrayStatisticsCompute for BitmapStats {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        let bitset = self
            .0
            .to_bitset()
            .ok_or_else(|| vortex_err!("Bitmap to Bitset conversion run out of memory"))?;
        let bitset_slice = bitset.as_slice();
        if bitset_slice.is_empty() {
            return Ok(StatsSet::from(HashMap::from([
                (Stat::Min, false.into()),
                (Stat::Max, false.into()),
                (Stat::IsConstant, true.into()),
                (Stat::IsSorted, true.into()),
                (Stat::IsStrictSorted, (self.1 == 1).into()),
                (Stat::RunCount, 1.into()),
                (Stat::TrueCount, 0.into()),
            ])));
        }

        let whole_chunks = self.1 / 64;
        let last_chunk_len = self.1 % 64;
        let fist_bool = bitset_slice[0] & 1 == 1;
        let mut stats = RoaringBoolStatsAccumulator::new(fist_bool);
        for bits64 in bitset_slice[0..whole_chunks].iter() {
            stats.next(*bits64);
        }
        if !bitset_slice.is_empty() {
            stats.next_up_to_length(bitset_slice[whole_chunks], last_chunk_len);
        }
        Ok(stats.finish())
    }
}

struct RoaringBoolStatsAccumulator {
    last: bool,
    is_sorted: bool,
    run_count: usize,
    true_count: usize,
    len: usize,
}

impl RoaringBoolStatsAccumulator {
    fn new(first_value: bool) -> Self {
        Self {
            last: first_value,
            is_sorted: true,
            run_count: 1,
            true_count: 0,
            len: 0,
        }
    }

    pub fn next_up_to_length(&mut self, next: u64, len: usize) {
        assert!(len <= 64);
        self.len += len;
        for i in 0..len {
            let current = ((next >> i) & 1) == 1;
            if current {
                self.true_count += 1;
            }
            if !current & self.last {
                self.is_sorted = false;
            }
            if current != self.last {
                self.run_count += 1;
                self.last = current;
            }
        }
    }

    pub fn next(&mut self, next: u64) {
        self.next_up_to_length(next, 64)
    }

    pub fn finish(self) -> StatsSet {
        StatsSet::from(HashMap::from([
            (Stat::Min, (self.true_count == self.len).into()),
            (Stat::Max, (self.true_count > 0).into()),
            (
                Stat::IsConstant,
                (self.true_count == self.len || self.true_count == 0).into(),
            ),
            (Stat::IsSorted, self.is_sorted.into()),
            (
                Stat::IsStrictSorted,
                (self.is_sorted && (self.len < 2 || (self.len == 2 && self.true_count == 1)))
                    .into(),
            ),
            (Stat::RunCount, self.run_count.into()),
            (Stat::TrueCount, self.true_count.into()),
        ]))
    }
}

#[cfg(test)]
mod test {
    use vortex::array::BoolArray;
    use vortex::stats::ArrayStatistics;
    use vortex::IntoArray;

    use crate::RoaringBoolArray;

    #[test]
    fn bool_stats() {
        let bool_arr = RoaringBoolArray::encode(
            BoolArray::from(vec![false, false, true, true, false, true, true, false]).into_array(),
        )
        .unwrap();
        assert!(!bool_arr.statistics().compute_is_strict_sorted().unwrap());
        assert!(!bool_arr.statistics().compute_is_sorted().unwrap());
        assert!(!bool_arr.statistics().compute_is_constant().unwrap());
        assert!(!bool_arr.statistics().compute_min::<bool>().unwrap());
        assert!(bool_arr.statistics().compute_max::<bool>().unwrap());
        assert_eq!(bool_arr.statistics().compute_run_count().unwrap(), 5);
        assert_eq!(bool_arr.statistics().compute_true_count().unwrap(), 4);
    }

    #[test]
    fn strict_sorted() {
        let bool_arr_1 =
            RoaringBoolArray::encode(BoolArray::from(vec![false, true]).into_array()).unwrap();
        assert!(bool_arr_1.statistics().compute_is_strict_sorted().unwrap());
        assert!(bool_arr_1.statistics().compute_is_sorted().unwrap());

        // let bool_arr_2 =
        //     RoaringBoolArray::encode(BoolArray::from(vec![true]).into_array()).unwrap();
        // assert!(bool_arr_2.statistics().compute_is_strict_sorted().unwrap());
        // assert!(bool_arr_2.statistics().compute_is_sorted().unwrap());

        let bool_arr_3 =
            RoaringBoolArray::encode(BoolArray::from(vec![false]).into_array()).unwrap();
        assert!(bool_arr_3.statistics().compute_is_strict_sorted().unwrap());
        assert!(bool_arr_3.statistics().compute_is_sorted().unwrap());

        // let bool_arr_4 =
        //     RoaringBoolArray::encode(BoolArray::from(vec![true, false]).into_array()).unwrap();
        // assert!(!bool_arr_4.statistics().compute_is_strict_sorted().unwrap());
        // assert!(!bool_arr_4.statistics().compute_is_sorted().unwrap());
        //
        let bool_arr_5 =
            RoaringBoolArray::encode(BoolArray::from(vec![false, true, true]).into_array())
                .unwrap();
        assert!(!bool_arr_5.statistics().compute_is_strict_sorted().unwrap());
        assert!(bool_arr_5.statistics().compute_is_sorted().unwrap());
    }
}
