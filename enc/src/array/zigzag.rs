use std::any::Any;
use std::sync::{Arc, RwLock};

use crate::array::{Array, ArrayKind, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};
use crate::compute::compress::zigzag::zigzag_encode;
use crate::error::{EncError, EncResult};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::Scalar;
use crate::stats::{Stats, StatsSet};
use crate::types::{DType, Signedness};

#[derive(Debug, Clone)]
pub struct ZigZagArray {
    encoded: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl ZigZagArray {
    pub fn try_new(encoded: ArrayRef) -> EncResult<Self> {
        let dtype = match encoded.dtype() {
            DType::Int(width, Signedness::Unsigned, nullability) => {
                DType::Int(width.clone(), Signedness::Signed, nullability.clone())
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

    fn scalar_at(&self, _index: usize) -> EncResult<Box<dyn Scalar>> {
        todo!()
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

pub const ZIGZAG_ENCODING: EncodingId = EncodingId("enc.zigzag");

impl Encoding for ZigZagEncoding {
    fn id(&self) -> &EncodingId {
        &ZIGZAG_ENCODING
    }
}
