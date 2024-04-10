use std::collections::HashMap;

use vortex::scalar::Scalar;
use vortex_error::VortexResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stat {
    BitWidthFreq,
    TrailingZeroFreq,
    IsConstant,
    IsSorted,
    IsStrictSorted,
    Max,
    Min,
    RunCount,
    TrueCount,
    NullCount,
}

pub trait ArrayStatistics {
    fn statistics(&self) -> &(dyn Statistics + '_) {
        &EmptyStatistics
    }
}

pub trait ArrayStatisticsCompute {
    /// Compute the requested statistic. Can return additional stats.
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<HashMap<Stat, Scalar>> {
        Ok(HashMap::new())
    }
}

pub trait Statistics {
    fn compute(&self, stat: Stat) -> Option<Scalar>;
    fn get(&self, stat: Stat) -> Option<Scalar>;
    fn set(&self, stat: Stat, value: Scalar);
    fn to_map(&self) -> HashMap<Stat, Scalar>;
}

impl dyn Statistics + '_ {
    pub fn compute_as<T: TryFrom<Scalar>>(&self, stat: Stat) -> Option<T> {
        self.compute(stat).and_then(|s| T::try_from(s).ok())
    }

    pub fn get_as<T: TryFrom<Scalar>>(&self, stat: Stat) -> Option<T> {
        self.get(stat).and_then(|s| T::try_from(s).ok())
    }
}

pub struct EmptyStatistics;
impl Statistics for EmptyStatistics {
    fn compute(&self, _stat: Stat) -> Option<Scalar> {
        None
    }
    fn get(&self, _stat: Stat) -> Option<Scalar> {
        None
    }
    fn set(&self, _stat: Stat, _value: Scalar) {}
    fn to_map(&self) -> HashMap<Stat, Scalar> {
        HashMap::default()
    }
}
