// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::iter;
use std::sync::{Arc, RwLock};

use arrow::array::{ArrayRef as ArrowArrayRef, AsArray, BooleanArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use linkme::distributed_slice;

use crate::arrow::CombineChunks;
use crate::compress::EncodingCompression;
use crate::dtype::{DType, Nullability};
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::{NullableScalar, Scalar};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stat, Stats, StatsSet};

use super::{
    check_index_bounds, check_slice_bounds, check_validity_buffer, Array, ArrayRef, ArrowIterator,
    Encoding, EncodingId, EncodingRef, ENCODINGS,
};

mod compress;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct BoolArray {
    buffer: BooleanBuffer,
    stats: Arc<RwLock<StatsSet>>,
    validity: Option<ArrayRef>,
}

impl BoolArray {
    pub fn new(buffer: BooleanBuffer, validity: Option<ArrayRef>) -> Self {
        Self::try_new(buffer, validity).unwrap()
    }

    pub fn try_new(buffer: BooleanBuffer, validity: Option<ArrayRef>) -> VortexResult<Self> {
        let validity = validity.filter(|v| !v.is_empty());
        check_validity_buffer(validity.as_deref())?;

        Ok(Self {
            buffer,
            stats: Arc::new(RwLock::new(StatsSet::new())),
            validity,
        })
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity
            .as_ref()
            .map(|v| v.scalar_at(index).unwrap().try_into().unwrap())
            .unwrap_or(true)
    }

    #[inline]
    pub fn buffer(&self) -> &BooleanBuffer {
        &self.buffer
    }

    #[inline]
    pub fn validity(&self) -> Option<&dyn Array> {
        self.validity.as_deref()
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
        if self.validity().is_some() {
            &DType::Bool(Nullability::Nullable)
        } else {
            &DType::Bool(Nullability::NonNullable)
        }
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        check_index_bounds(self, index)?;

        if self.is_valid(index) {
            Ok(self.buffer.value(index).into())
        } else {
            Ok(NullableScalar::none(self.dtype().clone()).boxed())
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(iter::once(Arc::new(BooleanArray::new(
            self.buffer.clone(),
            self.validity().map(|v| {
                NullBuffer::new(
                    v.iter_arrow()
                        .combine_chunks()
                        .as_boolean()
                        .values()
                        .clone(),
                )
            }),
        )) as ArrowArrayRef))
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(Self {
            buffer: self.buffer.slice(start, stop - start),
            stats: Arc::new(RwLock::new(StatsSet::new())),
            validity: self
                .validity
                .as_ref()
                .map(|v| v.slice(start, stop))
                .transpose()?,
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

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

#[derive(Debug)]
pub struct BoolEncoding;

impl BoolEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.bool");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_BOOL: EncodingRef = &BoolEncoding;

impl Encoding for BoolEncoding {
    fn id(&self) -> &EncodingId {
        &Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
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
        BoolArray::new(BooleanBuffer::from(value), None)
    }
}

impl FromIterator<Option<bool>> for BoolArray {
    fn from_iter<I: IntoIterator<Item = Option<bool>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut validity: Vec<bool> = Vec::with_capacity(lower);
        let values: Vec<bool> = iter
            .map(|i| {
                if let Some(v) = i {
                    validity.push(true);
                    v
                } else {
                    validity.push(false);
                    false
                }
            })
            .collect::<Vec<_>>();

        if validity.is_empty() {
            BoolArray::from(values)
        } else {
            BoolArray::new(
                BooleanBuffer::from(values),
                Some(BoolArray::from(validity).boxed()),
            )
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn slice() {
        let arr = BoolArray::from(vec![true, true, false, false, true])
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
            BoolArray::from(vec![true, true, false, false, true]).nbytes(),
            1
        );
    }
}
