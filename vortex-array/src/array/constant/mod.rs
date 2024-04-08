use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use linkme::distributed_slice;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::Scalar;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{ArrayStatistics, OwnedStats, Stat, Statistics, StatsSet};
use crate::validity::ArrayValidity;
use crate::validity::Validity;
use crate::{impl_array, ArrayWalker};

mod compute;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct ConstantArray {
    scalar: Scalar,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl ConstantArray {
    pub fn new<S>(scalar: S, length: usize) -> Self
    where
        Scalar: From<S>,
    {
        let scalar: Scalar = scalar.into();
        let stats = StatsSet::from(HashMap::from([
            (Stat::Max, scalar.clone()),
            (Stat::Min, scalar.clone()),
            (Stat::IsConstant, true.into()),
            (Stat::IsSorted, true.into()),
            (Stat::RunCount, 1.into()),
        ]));
        Self {
            scalar,
            length,
            stats: Arc::new(RwLock::new(stats)),
        }
    }

    pub fn scalar(&self) -> &Scalar {
        &self.scalar
    }
}

impl Array for ConstantArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.length
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.scalar.dtype()
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ConstantEncoding
    }

    fn nbytes(&self) -> usize {
        self.scalar.nbytes()
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        Ok(())
    }
}

impl ArrayDisplay for ConstantArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("scalar", self.scalar())
    }
}

impl ArrayValidity for ConstantArray {
    fn logical_validity(&self) -> Validity {
        match self.scalar().is_null() {
            true => Validity::Invalid(self.len()),
            false => Validity::Valid(self.len()),
        }
    }

    fn is_valid(&self, _index: usize) -> bool {
        match self.scalar.dtype().is_nullable() {
            true => !self.scalar().is_null(),
            false => true,
        }
    }
}

impl OwnedStats for ConstantArray {
    fn stats_set(&self) -> &RwLock<StatsSet> {
        &self.stats
    }
}

impl ArrayStatistics for ConstantArray {
    fn statistics(&self) -> &dyn Statistics {
        self
    }
}

#[derive(Debug)]
pub struct ConstantEncoding;

impl ConstantEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.constant");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_CONSTANT: EncodingRef = &ConstantEncoding;

impl Encoding for ConstantEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
