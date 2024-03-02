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

use vortex::array::{Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};
use vortex::compress::EncodingCompression;
use vortex::dtype::DType;
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::scalar::Scalar;
use vortex::serde::{ArraySerde, EncodingSerde, WriteCtx};
use vortex::stats::{Stats, StatsSet};

#[derive(Debug, Clone)]
pub struct GCDArray {
    shifted: ArrayRef,
    shift: u8,
    stats: Arc<RwLock<StatsSet>>,
}

impl GCDArray {
    pub fn new(shifted: ArrayRef, shift: u8) -> Self {
        Self::try_new(shifted, shift).unwrap()
    }

    pub fn try_new(shifted: ArrayRef, shift: u8) -> VortexResult<Self> {
        if !matches!(shifted.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(shifted.dtype().clone()));
        }
        Ok(Self {
            shifted,
            shift,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn shifted(&self) -> &dyn Array {
        self.shifted.as_ref()
    }

    #[inline]
    pub fn shift(&self) -> u8 {
        self.shift
    }
}

impl Array for GCDArray {
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
        self.shifted().len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.shifted().is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.shifted().dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        todo!()
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(GCDArray::new(self.shifted().slice(start, stop)?, self.shift).boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &GCDEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.encoded().nbytes() + self.patches().map(|p| p.nbytes()).unwrap_or(0)
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for GCDArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for GCDArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln(format!("shift: {}", self.shift(),))?;
        f.array(self.shifted())
    }
}

impl ArraySerde for GCDArray {
    fn write(&self, ctx: &mut WriteCtx) -> std::io::Result<()> {
        todo!()
    }
}

#[derive(Debug)]
pub struct GCDEncoding;

pub const GCD_ENCODING: EncodingId = EncodingId::new("vortex.gcd");

impl Encoding for GCDEncoding {
    fn id(&self) -> &EncodingId {
        &GCD_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
