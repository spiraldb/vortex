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
use std::sync::{Arc, RwLock};

use zigzag::ZigZag;

use vortex::array::{
    check_index_bounds, Array, ArrayKind, ArrayRef, ArrowIterator, Encoding, EncodingId,
    EncodingRef,
};
use vortex::compress::EncodingCompression;
use vortex::dtype::{DType, IntWidth, Signedness};
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::scalar::{NullableScalar, Scalar};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};

use crate::compress::zigzag_encode;

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

    pub fn try_new(encoded: ArrayRef) -> VortexResult<Self> {
        let dtype = match encoded.dtype() {
            DType::Int(width, Signedness::Unsigned, nullability) => {
                DType::Int(*width, Signedness::Signed, *nullability)
            }
            d => return Err(VortexError::InvalidDType(d.clone())),
        };
        Ok(Self {
            encoded,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn encode(array: &dyn Array) -> VortexResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(zigzag_encode(p)?.boxed()),
            _ => Err(VortexError::InvalidEncoding(array.encoding().id().clone())),
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

    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
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
            _ => Err(VortexError::InvalidDType(self.dtype().clone())),
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
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

impl ZigZagEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.zigzag");
}

impl Encoding for ZigZagEncoding {
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
