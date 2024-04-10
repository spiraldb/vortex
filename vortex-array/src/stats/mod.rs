use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::RwLock;

use enum_iterator::Sequence;
pub use statsset::*;
use vortex_error::{vortex_err, VortexError, VortexResult};
use vortex_schema::DType;

use crate::ptype::NativePType;
use crate::scalar::{ListScalarVec, Scalar};

mod statsset;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Sequence)]
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

impl Display for Stat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Stat::BitWidthFreq => write!(f, "bit_width_frequency"),
            Stat::TrailingZeroFreq => write!(f, "trailing_zero_frequency"),
            Stat::IsConstant => write!(f, "is_constant"),
            Stat::IsSorted => write!(f, "is_sorted"),
            Stat::IsStrictSorted => write!(f, "is_strict_sorted"),
            Stat::Max => write!(f, "max"),
            Stat::Min => write!(f, "min"),
            Stat::RunCount => write!(f, "run_count"),
            Stat::TrueCount => write!(f, "true_count"),
            Stat::NullCount => write!(f, "null_count"),
        }
    }
}

pub trait ArrayStatistics {
    fn statistics(&self) -> &dyn Statistics;
}

pub trait Statistics {
    /// Returns the value of the statistic only if it's present
    fn get(&self, stat: Stat) -> Option<Scalar>;

    /// Get all existing statistics
    fn get_all(&self) -> StatsSet;

    fn set(&self, stat: Stat, value: Scalar);

    fn set_many(&self, other: &dyn Statistics, stats: &[Stat]);

    /// Computes the value of the stat if it's not present
    fn compute(&self, stat: Stat) -> Option<Scalar>;
}

pub trait OwnedStats {
    fn stats_set(&self) -> &RwLock<StatsSet>;
}

pub trait StatsCompute {
    fn compute(&self, stat: Stat) -> VortexResult<StatsSet>;
}

impl<T: OwnedStats + StatsCompute> Statistics for T {
    fn get(&self, stat: Stat) -> Option<Scalar> {
        self.stats_set().read().unwrap().get(stat).cloned()
    }

    fn get_all(&self) -> StatsSet {
        self.stats_set().read().unwrap().clone()
    }

    fn set(&self, stat: Stat, value: Scalar) {
        self.stats_set().write().unwrap().set(stat, value);
    }

    fn set_many(&self, other: &dyn Statistics, stats: &[Stat]) {
        self.stats_set().write().unwrap().extend(
            stats
                .iter()
                .copied()
                .filter_map(|stat| other.get(stat).map(|s| (stat, s))),
        )
    }

    fn compute(&self, stat: Stat) -> Option<Scalar> {
        if let Some(s) = self.get(stat) {
            return Some(s);
        }

        self.stats_set()
            .write()
            .unwrap()
            .extend(self.compute(stat).unwrap());
        self.get(stat)
    }
}

pub trait StatsAccessors: Statistics {
    fn get_as<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
        stat: Stat,
    ) -> VortexResult<U> {
        self.get(stat)
            .ok_or_else(|| vortex_err!(ComputeError: "statistic {} missing", stat))
            .and_then(|v| U::try_from(&v))
    }

    fn compute_as<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
        stat: Stat,
    ) -> VortexResult<U> {
        self.compute(stat)
            .ok_or_else(|| vortex_err!(ComputeError: "statistic {} missing", stat))
            .and_then(|v| U::try_from(&v))
    }

    fn compute_as_cast<U: NativePType>(&self, stat: Stat) -> VortexResult<U> {
        self.compute(stat)
            .ok_or_else(|| vortex_err!(ComputeError: "statistic {} missing", stat))
            .and_then(|v| v.cast(&DType::from(U::PTYPE)))
            .and_then(|v| U::try_from(v))
    }

    fn compute_min<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(&self) -> VortexResult<U> {
        self.compute_as(Stat::Min)
    }

    fn compute_max<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(&self) -> VortexResult<U> {
        self.compute_as(Stat::Max)
    }

    fn compute_is_strict_sorted(&self) -> VortexResult<bool> {
        self.compute_as(Stat::IsStrictSorted)
    }

    fn compute_is_sorted(&self) -> VortexResult<bool> {
        self.compute_as(Stat::IsSorted)
    }

    fn compute_is_constant(&self) -> VortexResult<bool> {
        self.compute_as(Stat::IsConstant)
    }

    fn compute_true_count(&self) -> VortexResult<usize> {
        self.compute_as(Stat::TrueCount)
    }

    fn compute_null_count(&self) -> VortexResult<usize> {
        self.compute_as(Stat::NullCount)
    }

    fn compute_run_count(&self) -> VortexResult<usize> {
        self.compute_as(Stat::RunCount)
    }

    fn compute_bit_width_freq(&self) -> VortexResult<Vec<usize>> {
        self.compute_as::<ListScalarVec<usize>>(Stat::BitWidthFreq)
            .map(|s| s.0)
    }

    fn compute_trailing_zero_freq(&self) -> VortexResult<Vec<usize>> {
        self.compute_as::<ListScalarVec<usize>>(Stat::TrailingZeroFreq)
            .map(|s| s.0)
    }
}

impl StatsAccessors for dyn Statistics + '_ {}

impl<T: OwnedStats + StatsCompute> StatsAccessors for T {
    fn get_as<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
        stat: Stat,
    ) -> VortexResult<U> {
        with_stat_value(self, stat, |s| U::try_from(s))
    }

    fn compute_as<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
        stat: Stat,
    ) -> VortexResult<U> {
        with_computed_stat_value(self, stat, |s| U::try_from(s))
    }

    fn compute_as_cast<U: NativePType>(&self, stat: Stat) -> VortexResult<U> {
        with_computed_stat_value(self, stat, |s| {
            s.cast(&DType::from(U::PTYPE)).and_then(U::try_from)
        })
    }
}

fn with_stat_value<S: OwnedStats, U>(
    stats: &S,
    stat: Stat,
    f: impl Fn(&Scalar) -> VortexResult<U>,
) -> VortexResult<U> {
    stats
        .stats_set()
        .read()
        .unwrap()
        .get(stat)
        .ok_or_else(|| vortex_err!(ComputeError: "statistic {} missing", stat))
        .and_then(f)
}

fn with_computed_stat_value<S: OwnedStats + StatsCompute, U>(
    stats: &S,
    stat: Stat,
    f: impl Fn(&Scalar) -> VortexResult<U>,
) -> VortexResult<U> {
    if let Some(s) = stats.stats_set().read().unwrap().get(stat) {
        return f(s);
    }

    stats
        .stats_set()
        .write()
        .unwrap()
        .extend(stats.compute(stat).unwrap());
    with_stat_value(stats, stat, f)
}
