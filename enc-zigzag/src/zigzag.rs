use std::any::Any;
use std::sync::{Arc, RwLock};

use zigzag::ZigZag;

use crate::compress::zigzag_encode;
use enc::array::{
    check_index_bounds, Array, ArrayKind, ArrayRef, ArrowIterator, Encoding, EncodingId,
    EncodingRef,
};
use enc::compress::EncodingCompression;
use enc::dtype::{DType, IntWidth, Signedness};
use enc::error::{EncError, EncResult};
use enc::formatter::{ArrayDisplay, ArrayFormatter};
use enc::scalar::{NullableScalar, Scalar};
use enc::serde::{ArraySerde, EncodingSerde};
use enc::stats::{Stats, StatsSet};

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

    pub fn try_new(encoded: ArrayRef) -> EncResult<Self> {
        let dtype = match encoded.dtype() {
            DType::Int(width, Signedness::Unsigned, nullability) => {
                DType::Int(*width, Signedness::Signed, *nullability)
            }
            d => return Err(EncError::InvalidDType(d.clone())),
        };
        Ok(Self {
            encoded,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn encode(array: &dyn Array) -> EncResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(Self::try_new(zigzag_encode(p).boxed())?.boxed()),
            _ => Err(EncError::InvalidEncoding(array.encoding().id().clone())),
        }
    }

    pub fn encoded(&self) -> &dyn Array {
        self.encoded.as_ref()
    }
}

impl Array for ZigZagArray {
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
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        check_index_bounds(self, index)?;

        let scalar = self.encoded().scalar_at(index)?;
        let Some(scalar) = scalar.as_nonnull() else {
            return Ok(NullableScalar::none(self.dtype().clone()).boxed());
        };
        match self.dtype() {
            DType::Int(IntWidth::_8, Signedness::Signed, _) => {
                Ok(i8::decode(scalar.try_into()?).into())
            }
            DType::Int(IntWidth::_16, Signedness::Signed, _) => {
                Ok(i16::decode(scalar.try_into()?).into())
            }
            DType::Int(IntWidth::_32, Signedness::Signed, _) => {
                Ok(i32::decode(scalar.try_into()?).into())
            }
            DType::Int(IntWidth::_64, Signedness::Signed, _) => {
                Ok(i64::decode(scalar.try_into()?).into())
            }
            _ => Err(EncError::InvalidDType(self.dtype().clone())),
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        Ok(Self::try_new(self.encoded.slice(start, stop)?)?.boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ZigZagEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.encoded.nbytes()
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for ZigZagArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for ZigZagArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("zigzag:")?;
        f.indent(|indent| indent.array(self.encoded.as_ref()))
    }
}

#[derive(Debug)]
pub struct ZigZagEncoding;

pub const ZIGZAG_ENCODING: EncodingId = EncodingId::new("enc.zigzag");

impl Encoding for ZigZagEncoding {
    fn id(&self) -> &EncodingId {
        &ZIGZAG_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
