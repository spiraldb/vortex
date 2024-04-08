use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::RwLock;

use enum_iterator::{all, Sequence};
use itertools::Itertools;
use vortex_error::{vortex_err, VortexError, VortexResult};
use vortex_schema::DType;

use crate::ptype::NativePType;
use crate::scalar::{ListScalarVec, Scalar};

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
        self.stats_set().write().unwrap().values.extend(
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
            .values
            .extend(self.compute(stat).unwrap().values);
        self.get(stat)
    }
}

impl dyn Statistics + '_ {
    pub fn compute_as_cast<U: NativePType>(&self, stat: Stat) -> VortexResult<U> {
        self.compute(stat)
            .ok_or_else(|| vortex_err!(ComputeError: "statistic {} missing", stat))
            .and_then(|v| v.cast(&DType::from(U::PTYPE)))
            .and_then(|v| U::try_from(v))
    }

    fn compute_as<U: TryFrom<Scalar, Error = VortexError>>(&self, stat: Stat) -> VortexResult<U> {
        self.compute(stat)
            .ok_or_else(|| vortex_err!(ComputeError: "statistic {} missing", stat))
            .and_then(|v| U::try_from(v))
    }

    pub fn compute_min<U: TryFrom<Scalar, Error = VortexError>>(&self) -> VortexResult<U> {
        self.compute_as(Stat::Min)
    }

    pub fn compute_max<U: TryFrom<Scalar, Error = VortexError>>(&self) -> VortexResult<U> {
        self.compute_as(Stat::Max)
    }

    pub fn compute_is_strict_sorted(&self) -> VortexResult<bool> {
        self.compute_as(Stat::IsStrictSorted)
    }

    pub fn compute_is_sorted(&self) -> VortexResult<bool> {
        self.compute_as(Stat::IsSorted)
    }

    pub fn compute_is_constant(&self) -> VortexResult<bool> {
        self.compute_as(Stat::IsConstant)
    }

    pub fn compute_true_count(&self) -> VortexResult<usize> {
        self.compute_as(Stat::TrueCount)
    }

    pub fn compute_null_count(&self) -> VortexResult<usize> {
        self.compute_as(Stat::NullCount)
    }

    pub fn compute_run_count(&self) -> VortexResult<usize> {
        self.compute_as(Stat::RunCount)
    }

    pub fn compute_bit_width_freq(&self) -> VortexResult<Vec<usize>> {
        self.compute_as::<ListScalarVec<usize>>(Stat::BitWidthFreq)
            .map(|s| s.0)
    }

    pub fn compute_trailing_zero_freq(&self) -> VortexResult<Vec<usize>> {
        self.compute_as::<ListScalarVec<usize>>(Stat::TrailingZeroFreq)
            .map(|s| s.0)
    }
}

#[derive(Debug, Clone, Default)]
pub struct StatsSet {
    values: HashMap<Stat, Scalar>,
}

impl From<HashMap<Stat, Scalar>> for StatsSet {
    fn from(value: HashMap<Stat, Scalar>) -> Self {
        Self { values: value }
    }
}

impl StatsSet {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn of(stat: Stat, value: Scalar) -> Self {
        StatsSet::from(HashMap::from([(stat, value)]))
    }

    pub fn get(&self, stat: Stat) -> Option<&Scalar> {
        self.values.get(&stat)
    }

    fn get_as<T: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(&self, stat: Stat) -> Option<T> {
        self.get(stat).map(|v| T::try_from(v).unwrap())
    }

    pub fn set(&mut self, stat: Stat, value: Scalar) {
        self.values.insert(stat, value);
    }

    pub fn merge(&mut self, other: &Self) -> &Self {
        for s in all::<Stat>() {
            match s {
                Stat::BitWidthFreq => self.merge_bit_width_freq(other),
                Stat::TrailingZeroFreq => self.merge_trailing_zero_freq(other),
                Stat::IsConstant => self.merge_is_constant(other),
                Stat::IsSorted => self.merge_is_sorted(other),
                Stat::IsStrictSorted => self.merge_is_strict_sorted(other),
                Stat::Max => self.merge_max(other),
                Stat::Min => self.merge_min(other),
                Stat::RunCount => self.merge_run_count(other),
                Stat::TrueCount => self.merge_true_count(other),
                Stat::NullCount => self.merge_null_count(other),
            }
        }

        self
    }

    fn merge_min(&mut self, other: &Self) {
        self.merge_ordered(other, |other, own| other < own);
    }

    fn merge_max(&mut self, other: &Self) {
        self.merge_ordered(other, |other, own| other > own);
    }

    fn merge_ordered<F: Fn(&Scalar, &Scalar) -> bool>(&mut self, other: &Self, cmp: F) {
        match self.values.entry(Stat::Max) {
            Entry::Occupied(mut e) => {
                if let Some(omin) = other.get(Stat::Max) {
                    if cmp(omin, e.get()) {
                        e.insert(omin.clone());
                    }
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.get(Stat::Max) {
                    e.insert(min.clone());
                }
            }
        }
    }

    fn merge_is_constant(&mut self, other: &Self) {
        if let Some(is_constant) = self.get_as(Stat::IsConstant) {
            if let Some(other_is_constant) = other.get_as(Stat::IsConstant) {
                if is_constant
                    && other_is_constant
                    && self.values.get(&Stat::Min) == other.get(Stat::Min)
                {
                    return;
                }
            }
            self.values.insert(Stat::IsConstant, false.into());
        }
    }

    fn merge_is_sorted(&mut self, other: &Self) {
        self.merge_sortedness_stat(other, Stat::IsSorted, |own, other| own <= other)
    }

    fn merge_is_strict_sorted(&mut self, other: &Self) {
        self.merge_sortedness_stat(other, Stat::IsStrictSorted, |own, other| own < other)
    }

    fn merge_sortedness_stat<F: Fn(Option<&Scalar>, Option<&Scalar>) -> bool>(
        &mut self,
        other: &Self,
        stat: Stat,
        cmp: F,
    ) {
        if let Some(is_sorted) = self.get_as(stat) {
            if let Some(other_is_sorted) = other.get_as(stat) {
                if is_sorted && other_is_sorted && cmp(self.get(Stat::Max), other.get(Stat::Min)) {
                    return;
                }
            }
            self.values.insert(stat, false.into());
        }
    }

    fn merge_true_count(&mut self, other: &Self) {
        self.merge_scalar_stat(other, Stat::TrueCount)
    }

    fn merge_null_count(&mut self, other: &Self) {
        self.merge_scalar_stat(other, Stat::NullCount)
    }

    fn merge_scalar_stat(&mut self, other: &Self, stat: Stat) {
        match self.values.entry(stat) {
            Entry::Occupied(mut e) => {
                if let Some(other_value) = other.get_as::<usize>(stat) {
                    let self_value: usize = e.get().try_into().unwrap();
                    e.insert((self_value + other_value).into());
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.get(stat) {
                    e.insert(min.clone());
                }
            }
        }
    }

    fn merge_bit_width_freq(&mut self, other: &Self) {
        self.merge_freq_stat(other, Stat::BitWidthFreq)
    }

    fn merge_trailing_zero_freq(&mut self, other: &Self) {
        self.merge_freq_stat(other, Stat::TrailingZeroFreq)
    }

    fn merge_freq_stat(&mut self, other: &Self, stat: Stat) {
        match self.values.entry(stat) {
            Entry::Occupied(mut e) => {
                if let Some(other_value) = other.get_as::<ListScalarVec<u64>>(stat) {
                    // TODO(robert): Avoid the copy here. We could e.get_mut() but need to figure out casting
                    let self_value: ListScalarVec<u64> = e.get().try_into().unwrap();
                    e.insert(
                        ListScalarVec(
                            self_value
                                .0
                                .iter()
                                .zip_eq(other_value.0.iter())
                                .map(|(s, o)| *s + *o)
                                .collect::<Vec<_>>(),
                        )
                        .into(),
                    );
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.get(stat) {
                    e.insert(min.clone());
                }
            }
        }
    }

    /// Merged run count is an upper bound where we assume run is interrupted at the boundary
    fn merge_run_count(&mut self, other: &Self) {
        match self.values.entry(Stat::RunCount) {
            Entry::Occupied(mut e) => {
                if let Some(other_value) = other.get_as::<usize>(Stat::RunCount) {
                    let self_value: usize = e.get().try_into().unwrap();
                    e.insert((self_value + other_value + 1).into());
                }
            }
            Entry::Vacant(e) => {
                if let Some(min) = other.get(Stat::RunCount) {
                    e.insert(min.clone());
                }
            }
        }
    }
}
