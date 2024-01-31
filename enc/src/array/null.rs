use std::any::Any;
use std::sync::{Arc, RwLock};

use arrow::array::Array as ArrowArray;
use arrow::array::NullArray as ArrowNullArray;

use crate::array::formatter::{ArrayDisplay, ArrayFormatter};
use crate::array::stats::{Stats, StatsSet};
use crate::array::{Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};
use crate::error::EncResult;
use crate::scalar::{NullScalar, Scalar};
use crate::types::DType;

#[derive(Debug, Clone)]
pub struct NullArray {
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl NullArray {
    pub fn new(length: usize) -> Self {
        Self {
            length,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }
}

impl Array for NullArray {
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
        self.length
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &DType::Null
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, _index: usize) -> EncResult<Box<dyn Scalar>> {
        Ok(NullScalar::new().boxed())
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(std::iter::once(
            Arc::new(ArrowNullArray::new(self.length)) as Arc<dyn ArrowArray>
        ))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        self.check_slice_bounds(start, stop)?;

        let mut cloned = self.clone();
        cloned.length = stop - start;
        Ok(cloned.boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &NullEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        8
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for NullArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for NullArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("(null)")
    }
}

#[derive(Debug)]
pub struct NullEncoding;

pub const NULL_ENCODING: EncodingId = EncodingId("enc.null");

impl Encoding for NullEncoding {
    fn id(&self) -> &EncodingId {
        &NULL_ENCODING
    }
}
