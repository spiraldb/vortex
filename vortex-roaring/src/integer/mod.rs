use std::sync::{Arc, RwLock};

use compress::roaring_encode;
use croaring::{Bitmap, Native};
use vortex::array::{Array, ArrayKind, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::compute::ArrayCompute;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::ptype::PType;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{ArrayStatistics, OwnedStats, Statistics, StatsSet};
use vortex::validity::ArrayValidity;
use vortex::validity::Validity;
use vortex::{impl_array, ArrayWalker};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::DType;

mod compress;
mod compute;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct RoaringIntArray {
    bitmap: Bitmap,
    ptype: PType,
    stats: Arc<RwLock<StatsSet>>,
}

impl RoaringIntArray {
    pub fn new(bitmap: Bitmap, ptype: PType) -> Self {
        Self::try_new(bitmap, ptype).unwrap()
    }

    pub fn try_new(bitmap: Bitmap, ptype: PType) -> VortexResult<Self> {
        if !ptype.is_unsigned_int() {
            vortex_bail!("RoaringInt expected unsigned int");
        }

        Ok(Self {
            bitmap,
            ptype,
            stats: Arc::new(RwLock::new(StatsSet::default())),
        })
    }

    pub fn bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    pub fn ptype(&self) -> PType {
        self.ptype
    }

    pub fn encode(array: &dyn Array) -> VortexResult<Self> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(roaring_encode(p)),
            _ => Err(vortex_err!("RoaringInt can only encode primitive arrays")),
        }
    }
}

impl Array for RoaringIntArray {
    impl_array!();
    #[inline]
    fn len(&self) -> usize {
        self.bitmap.cardinality() as usize
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.bitmap().is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.ptype.into()
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &RoaringIntEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.bitmap.get_serialized_size_in_bytes::<Native>()
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
        todo!()
    }
}

impl ArrayValidity for RoaringIntArray {
    fn logical_validity(&self) -> Validity {
        Validity::Valid(self.len())
    }

    fn is_valid(&self, _index: usize) -> bool {
        true
    }
}

impl ArrayDisplay for RoaringIntArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("bitmap", format!("{:?}", self.bitmap()))
    }
}

impl OwnedStats for RoaringIntArray {
    fn stats_set(&self) -> &RwLock<StatsSet> {
        &self.stats
    }
}

impl ArrayStatistics for RoaringIntArray {
    fn statistics(&self) -> &dyn Statistics {
        self
    }
}

#[derive(Debug)]
pub struct RoaringIntEncoding;

impl RoaringIntEncoding {
    pub const ID: EncodingId = EncodingId::new("roaring.int");
}

impl Encoding for RoaringIntEncoding {
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

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::scalar_at::scalar_at;
    use vortex_error::VortexResult;

    use crate::RoaringIntArray;

    #[test]
    pub fn test_scalar_at() -> VortexResult<()> {
        let ints = PrimitiveArray::from(vec![2u32, 12, 22, 32]);
        let array = RoaringIntArray::encode(&ints)?;

        assert_eq!(scalar_at(&array, 0).unwrap(), 2u32.into());
        assert_eq!(scalar_at(&array, 1).unwrap(), 12u32.into());

        Ok(())
    }
}
