use std::any::Any;
use std::iter;
use std::sync::{Arc, RwLock};

use crate::array::formatter::{ArrayDisplay, ArrayFormatter};
use arrow::array::{ArrayRef as ArrowArrayRef, BooleanArray};
use arrow::buffer::BooleanBuffer;

use crate::array::stats::{Stat, Stats, StatsSet};
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

use super::{Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};

#[derive(Debug, Clone)]
pub struct BoolArray {
    buffer: BooleanBuffer,
    stats: Arc<RwLock<StatsSet>>,
}

impl BoolArray {
    pub fn new(buffer: BooleanBuffer) -> Self {
        Self {
            buffer,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    #[inline]
    pub fn buffer(&self) -> &BooleanBuffer {
        &self.buffer
    }
}

impl Array for BoolArray {
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
        self.buffer.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &DType::Bool
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        if index >= self.len() {
            return Err(EncError::OutOfBounds(index, 0, self.len()));
        }

        if self.buffer.value(index) {
            Ok(true.into())
        } else {
            Ok(false.into())
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(iter::once(
            Arc::new(BooleanArray::from(self.buffer.clone())) as ArrowArrayRef,
        ))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        self.check_slice_bounds(start, stop)?;

        Ok(Self {
            buffer: self.buffer.slice(start, stop - start),
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &BoolEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        (self.len() + 7) / 8
    }
}

#[derive(Debug)]
struct BoolEncoding;

pub const BOOL_ENCODING: EncodingId = EncodingId("enc.bool");

impl Encoding for BoolEncoding {
    fn id(&self) -> &EncodingId {
        &BOOL_ENCODING
    }
}

impl<'a> AsRef<(dyn Array + 'a)> for BoolArray {
    fn as_ref(&self) -> &(dyn Array + 'a) {
        self
    }
}

impl ArrayDisplay for BoolArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        let true_count = self.stats().get_or_compute_or(0usize, &Stat::TrueCount);
        let false_count = self.len() - true_count;
        f.writeln(format!("n_true: {}, n_false: {}", true_count, false_count))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn slice() {
        let arr = BoolArray::new(BooleanBuffer::from(vec![true, true, false, false, true]))
            .slice(1, 4)
            .unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(true));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(false));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(false));
    }

    #[test]
    fn nbytes() {
        assert_eq!(
            BoolArray::new(BooleanBuffer::from(vec![true, true, false, false, true])).nbytes(),
            1
        );
    }
}
