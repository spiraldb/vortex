use std::any::Any;
use std::sync::{Arc, RwLock};
use std::usize;

use crate::array::{Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};
use crate::compress::ArrayCompression;
use crate::dtype::DType;
use crate::error::EncResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::Scalar;
use crate::stats::{Stats, StatsSet};

mod stats;

#[derive(Debug, Clone)]
pub struct NullableArray {
    data: ArrayRef,
    validity: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl NullableArray {
    pub fn new(data: ArrayRef, validity: ArrayRef) -> Self {
        // Assert validity is uint array
        let dtype = data.dtype().as_nullable();
        Self {
            data,
            validity,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    #[inline]
    pub fn data(&self) -> &dyn Array {
        self.data.as_ref()
    }

    #[inline]
    pub fn validity(&self) -> &dyn Array {
        self.validity.as_ref()
    }
}

impl Array for NullableArray {
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
        self.data.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.data.is_empty()
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

    fn slice(&self, _start: usize, _stop: usize) -> EncResult<ArrayRef> {
        todo!()
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &NullableEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.data.nbytes() + self.validity.nbytes()
    }

    fn compression(&self) -> Option<&dyn ArrayCompression> {
        None
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for NullableArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for NullableArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("data:")?;
        f.indent(|indented| indented.array(self.data()))?;
        f.writeln("validity:")?;
        f.indent(|indented| indented.array(self.validity()))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct NullableEncoding;

pub const NULLABLE_ENCODING: EncodingId = EncodingId("enc.nullable");

impl Encoding for NullableEncoding {
    fn id(&self) -> &EncodingId {
        &NULLABLE_ENCODING
    }
}
