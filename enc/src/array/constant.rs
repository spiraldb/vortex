use std::any::Any;
use std::sync::{Arc, RwLock};

use arrow::array::Datum;

use crate::array::formatter::{ArrayDisplay, ArrayFormatter};
use crate::array::stats::{Stats, StatsSet};
use crate::array::{Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};
use crate::arrow::compute::repeat;
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone)]
pub struct ConstantArray {
    scalar: Box<dyn Scalar>,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl ConstantArray {
    pub fn new(scalar: Box<dyn Scalar>, length: usize) -> Self {
        Self {
            scalar,
            length,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    pub fn value(&self) -> &dyn Scalar {
        self.scalar.as_ref()
    }
}

impl Array for ConstantArray {
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
        self.scalar.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        if index >= self.length {
            return Err(EncError::OutOfBounds(index, 0, self.length));
        }
        Ok(self.scalar.clone())
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let arrow_scalar: Box<dyn Datum> = self.scalar.as_ref().into();
        Box::new(std::iter::once(repeat(arrow_scalar.as_ref(), self.length)))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        self.check_slice_bounds(start, stop)?;

        let mut cloned = self.clone();
        cloned.length = stop - start;
        Ok(Box::new(cloned))
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ConstantEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.scalar.nbytes()
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for ConstantArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for ConstantArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln(format!("{}", self.value()))
    }
}

#[derive(Debug)]
pub struct ConstantEncoding;

pub const CONSTANT_ENCODING: EncodingId = EncodingId("enc.constant");

impl Encoding for ConstantEncoding {
    fn id(&self) -> &EncodingId {
        &CONSTANT_ENCODING
    }
}
