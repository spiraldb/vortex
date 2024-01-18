use std::collections::HashMap;
use std::sync::RwLock;

use crate::scalar::Scalar;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Stat {
    Min,
    Max,
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

    pub fn get_present(&self, stat: Stat) -> Option<Box<dyn Scalar>> {
        self.cache.read().unwrap().get(&stat).cloned()
    }

    pub fn maybe_get(&self, stat: Stat) -> Option<Box<dyn Scalar>> {
        if let Some(value) = self.cache.read().unwrap().get(&stat) {
            return Some(value.clone());
        }

        self.cache
            .write()
            .unwrap()
            .extend(self.compute.compute(stat.clone()));
        self.cache.read().unwrap().get(&stat).cloned()
    }
}
