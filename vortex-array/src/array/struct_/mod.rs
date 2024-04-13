use std::sync::{Arc, RwLock};

use linkme::distributed_slice;
use vortex_error::VortexResult;
use vortex_schema::{DType, FieldNames};

use super::{Array, ArrayRef};
use crate::compress::EncodingCompression;
use crate::compute::ArrayCompute;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{ArrayStatistics, OwnedStats, Stat, Statistics, StatsCompute, StatsSet};
use crate::validity::ArrayValidity;
use crate::validity::Validity;
use crate::{impl_array, ArrayWalker};

mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct StructArray {
    fields: Vec<ArrayRef>,
    dtype: DType,
    len: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl StructArray {
    pub fn new(names: FieldNames, fields: Vec<ArrayRef>, len: usize) -> Self {
        assert!(
            fields.iter().all(|v| v.len() == len),
            "Fields didn't have the same length"
        );
        let dtype = DType::Struct(names, fields.iter().map(|a| a.dtype().clone()).collect());
        Self {
            fields,
            dtype,
            len,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    #[inline]
    pub fn fields(&self) -> &[ArrayRef] {
        &self.fields
    }

    pub fn names(&self) -> &FieldNames {
        if let DType::Struct(names, _fields) = self.dtype() {
            names
        } else {
            panic!("dtype is not a struct")
        }
    }

    pub fn field_dtypes(&self) -> &[DType] {
        if let DType::Struct(_names, fields) = self.dtype() {
            fields
        } else {
            panic!("dtype is not a struct")
        }
    }
}

impl Array for StructArray {
    impl_array!();

    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &StructEncoding
    }

    fn nbytes(&self) -> usize {
        self.fields.iter().map(|arr| arr.nbytes()).sum()
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

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        for field in self.fields() {
            walker.visit_child(field)?;
        }
        Ok(())
    }
}

impl StatsCompute for StructArray {
    fn compute(&self, _stat: Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::new())
    }
}

impl ArrayValidity for StructArray {
    fn logical_validity(&self) -> Validity {
        todo!()
    }

    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }
}

impl OwnedStats for StructArray {
    fn stats_set(&self) -> &RwLock<StatsSet> {
        &self.stats
    }
}

impl ArrayStatistics for StructArray {
    fn statistics(&self) -> &dyn Statistics {
        self
    }
}

#[derive(Debug)]
pub struct StructEncoding;

impl StructEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.struct");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_STRUCT: EncodingRef = &StructEncoding;

impl Encoding for StructEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

impl ArrayDisplay for StructArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        let DType::Struct(n, _) = self.dtype() else {
            unreachable!()
        };
        for (name, field) in n.iter().zip(self.fields()) {
            f.child(&format!("\"{}\"", name), field.as_ref())?;
        }
        Ok(())
    }
}
