use std::any::Any;
use std::sync::{Arc, RwLock};

use croaring::{Bitmap, Native};

use compress::roaring_encode;
use enc::array::{
    check_index_bounds, check_slice_bounds, Array, ArrayKind, ArrayRef, ArrowIterator, Encoding,
    EncodingId, EncodingRef,
};
use enc::compress::{ArrayCompression, EncodingCompression};
use enc::dtype::Nullability::NonNullable;
use enc::dtype::Signedness::Signed;
use enc::dtype::{DType, IntWidth};
use enc::error::{EncError, EncResult};
use enc::formatter::{ArrayDisplay, ArrayFormatter};
use enc::scalar::Scalar;
use enc::stats::{Stats, StatsSet};

mod compress;
mod stats;

#[derive(Debug, Clone)]
pub struct RoaringIntArray {
    bitmap: Bitmap,
    stats: Arc<RwLock<StatsSet>>,
}

impl RoaringIntArray {
    pub fn new(bitmap: Bitmap) -> Self {
        Self {
            bitmap,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    pub fn bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    pub fn encode(array: &dyn Array) -> EncResult<Self> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(roaring_encode(p)),
            _ => Err(EncError::InvalidEncoding(array.encoding().id().clone())),
        }
    }
}

impl Array for RoaringIntArray {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

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
        &DType::Int(IntWidth::_32, Signed, NonNullable)
    }

    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        check_index_bounds(self, index)?;
        // Unwrap since we know the index is valid
        Ok(self.bitmap.select(index as u32).unwrap().into())
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;
        todo!()
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &RoaringIntEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.bitmap.get_serialized_size_in_bytes::<Native>()
    }

    fn compression(&self) -> Option<&dyn ArrayCompression> {
        Some(self)
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for RoaringIntArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for RoaringIntArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("roaring:")?;
        f.indent(|indent| indent.writeln(format!("{:?}", self.bitmap)))
    }
}

#[derive(Debug)]
pub struct RoaringIntEncoding;

pub const ROARING_INT_ENCODING: EncodingId = EncodingId::new("roaring.int");

impl Encoding for RoaringIntEncoding {
    fn id(&self) -> &EncodingId {
        &ROARING_INT_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }
}

#[cfg(test)]
mod test {
    use enc::array::primitive::PrimitiveArray;
    use enc::array::Array;
    use enc::error::EncResult;

    use crate::RoaringIntArray;

    #[test]
    pub fn scalar_at() -> EncResult<()> {
        let ints: &dyn Array = &PrimitiveArray::from_vec::<u32>(vec![2, 12, 22, 32]);
        let array = RoaringIntArray::encode(ints)?;

        assert_eq!(array.scalar_at(0), Ok(2u32.into()));
        assert_eq!(array.scalar_at(1), Ok(12u32.into()));

        Ok(())
    }
}
