use std::any::Any;
use std::iter;
use std::sync::{Arc, RwLock};

use arrow::array::{ArrayRef as ArrowArrayRef, BooleanArray};
use arrow::buffer::BooleanBuffer;

use crate::error::EncResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::Scalar;
use crate::stats::{Stat, Stats, StatsSet};
use crate::types::{DType, Nullability};

use super::{
    check_index_bounds, check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId,
    EncodingRef,
};

#[derive(Debug, Clone)]
pub struct BoolArray {
    buffer: BooleanBuffer,
    stats: Arc<RwLock<StatsSet>>,
    validity: Option<ArrayRef>,
}

impl BoolArray {
    pub fn new(buffer: BooleanBuffer) -> Self {
        Self {
            buffer,
            stats: Arc::new(RwLock::new(StatsSet::new())),
            validity: None,
        }
    }

    #[inline]
    pub fn buffer(&self) -> &BooleanBuffer {
        &self.buffer
    }

    #[inline]
    pub fn validity(&self) -> Option<&ArrayRef> {
        self.validity.as_ref()
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
        if self.validity.is_some() {
            &DType::Bool(Nullability::Nullable)
        } else {
            &DType::Bool(Nullability::NonNullable)
        }
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        check_index_bounds(self, index)?;

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
        check_slice_bounds(self, start, stop)?;

        Ok(Self {
            buffer: self.buffer.slice(start, stop - start),
            stats: Arc::new(RwLock::new(StatsSet::new())),
            validity: self.validity.clone(),
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

impl From<Vec<bool>> for BoolArray {
    fn from(value: Vec<bool>) -> Self {
        BoolArray::new(BooleanBuffer::from(value))
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
