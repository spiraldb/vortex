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

use core::cmp::min;
use std::any::Any;
use std::iter;
use std::mem::size_of;
use std::panic::RefUnwindSafe;
use std::ptr::NonNull;
use std::sync::{Arc, RwLock};

use allocator_api2::alloc::Allocator;
use arrow::alloc::ALIGNMENT as ARROW_ALIGNMENT;
use arrow::array::{make_array, ArrayData, AsArray};
use arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use linkme::distributed_slice;
use log::debug;

use crate::array::bool::BoolArray;
use crate::array::{
    check_index_bounds, check_slice_bounds, check_validity_buffer, Array, ArrayRef, ArrowIterator,
    Encoding, EncodingId, EncodingRef, ENCODINGS,
};
use crate::arrow::CombineChunks;
use crate::compress::EncodingCompression;
use crate::compute::ArrayCompute;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::ptype::{match_each_native_ptype, NativePType, PType};
use crate::scalar::{NullableScalar, Scalar};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsSet};

mod compress;
mod compute;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct PrimitiveArray {
    buffer: Buffer,
    ptype: PType,
    dtype: DType,
    validity: Option<ArrayRef>,
    stats: Arc<RwLock<StatsSet>>,
}

impl PrimitiveArray {
    pub fn new(ptype: PType, buffer: Buffer, validity: Option<ArrayRef>) -> Self {
        Self::try_new(ptype, buffer, validity).unwrap()
    }

    pub fn try_new(ptype: PType, buffer: Buffer, validity: Option<ArrayRef>) -> VortexResult<Self> {
        let validity = validity.filter(|v| !v.is_empty());
        check_validity_buffer(validity.as_deref())?;
        let dtype = if validity.is_some() {
            DType::from(ptype).as_nullable()
        } else {
            DType::from(ptype)
        };

        if buffer.as_ptr().align_offset(ARROW_ALIGNMENT) != 0 {
            debug!(
                "Arrow buffer is not aligned to {} bytes and thus may require a copy to realign.",
                ARROW_ALIGNMENT
            );
        }

        Ok(Self {
            buffer,
            ptype,
            dtype,
            validity,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn from_vec<T: NativePType>(values: Vec<T>) -> Self {
        Self::from_nullable(values, None)
    }

    /// Allocate buffer from allocator-api2 vector. This would be easier when arrow gets https://github.com/apache/arrow-rs/issues/3960
    #[inline]
    pub fn from_vec_in<T: NativePType, A: Allocator + RefUnwindSafe + Send + Sync + 'static>(
        values: allocator_api2::vec::Vec<T, A>,
    ) -> Self {
        Self::from_nullable_in(values, None)
    }

    pub fn from_nullable_in<
        T: NativePType,
        A: Allocator + RefUnwindSafe + Send + Sync + 'static,
    >(
        values: allocator_api2::vec::Vec<T, A>,
        validity: Option<ArrayRef>,
    ) -> Self {
        let ptr = values.as_ptr();
        let buffer = unsafe {
            Buffer::from_custom_allocation(
                NonNull::new(ptr as _).unwrap(),
                values.len() * size_of::<T>(),
                Arc::new(values),
            )
        };
        Self::new(T::PTYPE, buffer, validity)
    }

    pub fn from_nullable<T: NativePType>(values: Vec<T>, validity: Option<ArrayRef>) -> Self {
        let buffer = Buffer::from_vec::<T>(values);
        Self::new(T::PTYPE, buffer, validity)
    }

    pub fn is_valid(&self, index: usize) -> bool {
        self.validity
            .as_ref()
            .map(|v| v.scalar_at(index).unwrap().try_into().unwrap())
            .unwrap_or(true)
    }

    #[inline]
    pub fn ptype(&self) -> &PType {
        &self.ptype
    }

    #[inline]
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    #[inline]
    pub fn validity(&self) -> Option<&dyn Array> {
        self.validity.as_deref()
    }

    pub fn scalar_buffer<T: NativePType>(&self) -> ScalarBuffer<T> {
        ScalarBuffer::from(self.buffer().clone())
    }

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        if self.ptype() != &T::PTYPE {
            panic!("Invalid PType")
        }
        self.buffer().typed_data()
    }
}

impl Array for PrimitiveArray {
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
        self.buffer.len() / self.ptype.byte_width()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        check_index_bounds(self, index)?;

        if self.is_valid(index) {
            Ok(
                match_each_native_ptype!(self.ptype, |$T| self.buffer.typed_data::<$T>()
                    .get(index)
                    .unwrap()
                    .clone()
                    .into()
                ),
            )
        } else {
            Ok(NullableScalar::none(self.dtype().clone()).boxed())
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(iter::once(make_array(
            ArrayData::builder(self.dtype().into())
                .len(self.len())
                .nulls(self.validity().map(|v| {
                    NullBuffer::new(
                        v.iter_arrow()
                            .combine_chunks()
                            .as_boolean()
                            .values()
                            .clone(),
                    )
                }))
                .add_buffer(self.buffer.clone())
                .build()
                .unwrap(),
        )))
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        let byte_start = start * self.ptype.byte_width();
        let byte_length = (stop - start) * self.ptype.byte_width();

        Ok(Self {
            buffer: self.buffer.slice_with_length(byte_start, byte_length),
            ptype: self.ptype,
            validity: self
                .validity
                .as_ref()
                .map(|v| v.slice(start, stop))
                .transpose()?,
            dtype: self.dtype.clone(),
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &PrimitiveEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.buffer.len()
    }

    fn compute(&self) -> Option<&dyn ArrayCompute> {
        Some(self)
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for PrimitiveArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

#[derive(Debug)]
pub struct PrimitiveEncoding;

impl PrimitiveEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.primitive");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_PRIMITIVE: EncodingRef = &PrimitiveEncoding;

impl Encoding for PrimitiveEncoding {
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

/// Wrapper struct to create primitive array from Vec<Option<T>>, this would conflict with Vec<T>
pub struct NullableVec<T>(Vec<Option<T>>);

impl<T: NativePType> From<NullableVec<T>> for ArrayRef {
    fn from(value: NullableVec<T>) -> Self {
        PrimitiveArray::from_iter(value.0).boxed()
    }
}

impl<T: NativePType> From<Vec<T>> for ArrayRef {
    fn from(values: Vec<T>) -> Self {
        PrimitiveArray::from_vec(values).boxed()
    }
}

impl<T: NativePType> FromIterator<Option<T>> for PrimitiveArray {
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut validity: Vec<bool> = Vec::with_capacity(lower);
        let values: Vec<T> = iter
            .map(|i| {
                if let Some(v) = i {
                    validity.push(true);
                    v
                } else {
                    validity.push(false);
                    T::default()
                }
            })
            .collect::<Vec<_>>();

        if validity.is_empty() {
            PrimitiveArray::from_vec(values)
        } else {
            PrimitiveArray::from_nullable(values, Some(BoolArray::from(validity).boxed()))
        }
    }
}

impl ArrayDisplay for PrimitiveArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        match_each_native_ptype!(self.ptype(), |$P| {
            f.writeln(format!("{:?}{}",
                &self.buffer().typed_data::<$P>()[..min(10, self.len())],
                if self.len() > 10 { "..." } else { "" }))
        })
    }
}

#[cfg(test)]
mod test {
    use crate::dtype::{IntWidth, Nullability, Signedness};

    use super::*;

    #[test]
    fn from_arrow() {
        let arr = PrimitiveArray::from_vec::<i32>(vec![1, 2, 3]);
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.ptype, PType::I32);
        assert_eq!(
            arr.dtype(),
            &DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable)
        );

        // Ensure we can fetch the scalar at the given index.
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(1));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(3));
    }

    #[test]
    fn slice() {
        let arr = PrimitiveArray::from_vec(vec![1, 2, 3, 4, 5])
            .slice(1, 4)
            .unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(3));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(4));
    }
}
