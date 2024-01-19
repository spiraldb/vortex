use std::collections::HashMap;
use std::sync::RwLock;

use crate::error;
use crate::error::EncResult;
use crate::scalar::Scalar;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Stat {
    BitWidthFreq,
    IsConstant,
    IsSorted,
    Max,
    Min,
    RunCount,
}

pub type StatsSet = HashMap<Stat, Box<dyn Scalar>>;

pub trait StatsCompute {
    fn compute(&self, stat: Stat) -> StatsSet;
}

pub struct Stats<'a> {
    cache: &'a RwLock<StatsSet>,
    compute: &'a dyn StatsCompute,
}

impl<'a> Stats<'a> {
    pub fn new(cache: &'a RwLock<StatsSet>, compute: &'a dyn StatsCompute) -> Self {
        Self { cache, compute }
    }

    pub fn get(&self, stat: Stat) -> Option<Box<dyn Scalar>> {
        self.cache.read().unwrap().get(&stat).cloned()
    }

    pub fn get_as<T: TryFrom<Box<dyn Scalar>, Error = error::EncError>>(
        &self,
        stat: Stat,
    ) -> EncResult<Option<T>> {
        self.get(stat).map(|v| T::try_from(v)).transpose()
    }

    pub fn get_or_compute(&self, stat: Stat) -> Option<Box<dyn Scalar>> {
        if let Some(value) = self.cache.read().unwrap().get(&stat) {
            return Some(value.clone());
        }

        self.cache
            .write()
            .unwrap()
            .extend(self.compute.compute(stat.clone()));
        self.get(stat)
    }

    pub fn get_or_compute_as<T: TryFrom<Box<dyn Scalar>, Error = error::EncError>>(
        &self,
        stat: Stat,
    ) -> EncResult<Option<T>> {
        self.get_or_compute(stat)
            .map(|v| T::try_from(v))
            .transpose()
    }
}
