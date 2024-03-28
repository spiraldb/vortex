use std::sync::{Arc, RwLock};

use vortex::array::{Array, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::impl_array;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};
use vortex::validity::{ArrayValidity, Validity};
use vortex_error::VortexResult;
use vortex_schema::DType;

mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct BitPackedArray {
    encoded: ArrayRef,
    validity: Option<Validity>,
    patches: Option<ArrayRef>,
    len: usize,
    bit_width: usize,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl BitPackedArray {
    pub fn try_new(
        encoded: ArrayRef,
        validity: Option<Validity>,
        patches: Option<ArrayRef>,
        bit_width: usize,
        dtype: DType,
        len: usize,
    ) -> VortexResult<Self> {
        if let Some(v) = &validity {
            assert_eq!(v.len(), len);
        }
        // TODO(ngates): check encoded has type u8

        Ok(Self {
            encoded,
            validity,
            patches,
            bit_width,
            len,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn encoded(&self) -> &ArrayRef {
        &self.encoded
    }

    #[inline]
    pub fn bit_width(&self) -> usize {
        self.bit_width
    }

    #[inline]
    pub fn patches(&self) -> Option<&ArrayRef> {
        self.patches.as_ref()
    }
}

impl Array for BitPackedArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        unimplemented!("BitPackedArray::slice")
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &BitPackedEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        // Ignore any overheads like padding or the bit-width flag.
        let packed_size = ((self.bit_width * self.len()) + 7) / 8;
        packed_size
            + self.patches().map(|p| p.nbytes()).unwrap_or(0)
            + self.validity().map(|v| v.nbytes()).unwrap_or(0)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl ArrayDisplay for BitPackedArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("packed", format!("u{}", self.bit_width()))?;
        f.child("encoded", self.encoded())?;
        f.maybe_child("patches", self.patches())?;
        f.validity(self.validity())
    }
}

impl ArrayValidity for BitPackedArray {
    fn validity(&self) -> Option<Validity> {
        self.validity.clone()
    }
}

impl StatsCompute for BitPackedArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::default())
    }
}

#[derive(Debug)]
pub struct BitPackedEncoding;

impl BitPackedEncoding {
    pub const ID: EncodingId = EncodingId::new("fastlanes.bitpacked");
}

impl Encoding for BitPackedEncoding {
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
