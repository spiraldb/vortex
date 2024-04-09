use std::sync::{Arc, RwLock};

use vortex::array::{Array, ArrayKind, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::compute::ArrayCompute;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{ArrayStatistics, OwnedStats, Statistics, StatsSet};
use vortex::validity::{ArrayValidity, Validity};
use vortex::{impl_array, ArrayWalker};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::{DType, Signedness};

use crate::compress::zigzag_encode;

#[derive(Debug, Clone)]
pub struct ZigZagArray {
    encoded: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl ZigZagArray {
    pub fn new(encoded: ArrayRef) -> Self {
        Self::try_new(encoded).unwrap()
    }

    pub fn try_new(encoded: ArrayRef) -> VortexResult<Self> {
        let dtype = match encoded.dtype() {
            DType::Int(width, Signedness::Unsigned, nullability) => {
                DType::Int(*width, Signedness::Signed, *nullability)
            }
            d => vortex_bail!(MismatchedTypes: "unsigned int", d),
        };
        Ok(Self {
            encoded,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::default())),
        })
    }

    pub fn encode(array: &dyn Array) -> VortexResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(zigzag_encode(p)?.into_array()),
            _ => Err(vortex_err!("ZigZag can only encoding primitive arrays")),
        }
    }

    pub fn encoded(&self) -> &ArrayRef {
        &self.encoded
    }
}

impl Array for ZigZagArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.encoded.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.encoded.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ZigZagEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.encoded.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.encoded())
    }
}

impl ArrayValidity for ZigZagArray {
    fn logical_validity(&self) -> Validity {
        self.encoded().logical_validity()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.encoded().is_valid(index)
    }
}

impl ArrayDisplay for ZigZagArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("zigzag", self.encoded())
    }
}

impl OwnedStats for ZigZagArray {
    fn stats_set(&self) -> &RwLock<StatsSet> {
        &self.stats
    }
}

impl ArrayStatistics for ZigZagArray {
    fn statistics(&self) -> &dyn Statistics {
        self
    }
}

#[derive(Debug)]
pub struct ZigZagEncoding;

impl ZigZagEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.zigzag");
}

impl Encoding for ZigZagEncoding {
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
