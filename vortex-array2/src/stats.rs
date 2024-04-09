use std::collections::HashMap;

use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::Array;

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
    /// Compute the requested statistic. Can return additional stats.
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<HashMap<Stat, Scalar>> {
        Ok(HashMap::new())
    }
}

pub trait Statistics {
    fn compute(&self, stat: Stat) -> Option<Scalar>;
    fn get(&self, stat: Stat) -> Option<Scalar>;
    fn set(&self, stat: Stat, value: Scalar);
}

pub trait TypedStatistics {
    fn compute_as<T: TryFrom<Scalar>>(&self, stat: Stat) -> Option<T>;
    fn get_as<T: TryFrom<Scalar>>(&self, stat: Stat) -> Option<T>;
}

impl<S: Statistics> TypedStatistics for S {
    fn compute_as<T: TryFrom<Scalar>>(&self, _stat: Stat) -> Option<T> {
        // TODO(ngates): should we panic if conversion fails?
        todo!()
    }

    fn get_as<T: TryFrom<Scalar>>(&self, _stat: Stat) -> Option<T> {
        todo!()
    }
}

impl Statistics for Array<'_> {
    fn compute(&self, stat: Stat) -> Option<Scalar> {
        match self {
            Array::Data(d) => d.compute(stat),
            Array::DataRef(d) => d.compute(stat),
            Array::View(v) => v.compute(stat),
        }
    }

    fn get(&self, stat: Stat) -> Option<Scalar> {
        match self {
            Array::Data(d) => d.get(stat),
            Array::DataRef(d) => d.get(stat),
            Array::View(v) => v.get(stat),
        }
    }

    fn set(&self, stat: Stat, value: Scalar) {
        match self {
            Array::Data(d) => d.set(stat, value),
            Array::DataRef(d) => d.set(stat, value),
            Array::View(v) => v.set(stat, value),
        }
    }
}
