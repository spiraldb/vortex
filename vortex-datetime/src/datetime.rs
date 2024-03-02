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

use vortex::array::{
    check_index_bounds, check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId,
};
use vortex::compress::EncodingCompression;
use vortex::dtype::DType;
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::ptype::PType;
use vortex::scalar::Scalar;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};

/// An array that decomposes a datetime into days, seconds, and nanoseconds.
#[derive(Debug, Clone)]
pub struct DateTimeArray {
    days: ArrayRef,
    seconds: ArrayRef,
    nanoseconds: ArrayRef,
    stats: Arc<RwLock<StatsSet>>,
}

impl DateTimeArray {
    pub fn new(days: ArrayRef, seconds: ArrayRef, nanoseconds: ArrayRef) -> Self {
        Self::try_new(days, seconds, nanoseconds).unwrap()
    }

    pub fn try_new(days: ArrayRef, seconds: ArrayRef, nanoseconds: ArrayRef) -> VortexResult<Self> {
        if !matches!(days.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(days.dtype().clone()));
        }
        if !matches!(seconds.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(seconds.dtype().clone()));
        }
        if !matches!(nanoseconds.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(nanoseconds.dtype().clone()));
        }

        Ok(Self {
            days,
            seconds,
            nanoseconds,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn days(&self) -> &dyn Array {
        self.days.as_ref()
    }

    #[inline]
    pub fn seconds(&self) -> &dyn Array {
        self.seconds.as_ref()
    }

    #[inline]
    pub fn nanoseconds(&self) -> &dyn Array {
        self.nanoseconds.as_ref()
    }
}

impl Array for DateTimeArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn len(&self) -> usize {
        self.days.len()
    }

    fn is_empty(&self) -> bool {
        self.days.is_empty() && self.seconds.is_empty() && self.nanoseconds.is_empty()
    }

    fn dtype(&self) -> &DType {
        PType::I64.dtype()
    }

    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        check_index_bounds(self, index)?;
        let dict_index: usize = self.codes().scalar_at(index)?.try_into()?;
        self.dict().scalar_at(dict_index)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    // TODO(robert): Add function to trim the dictionary
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;
        Ok(Self::new(self.codes().slice(start, stop)?, self.dict.clone()).boxed())
    }

    fn encoding(&self) -> &'static dyn Encoding {
        &DateTimeEncoding
    }

    fn nbytes(&self) -> usize {
        self.codes().nbytes() + self.dict().nbytes()
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for DateTimeArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for DateTimeArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("dict:")?;
        f.indent(|indent| indent.array(self.dict()))?;
        f.writeln("codes:")?;
        f.indent(|indent| indent.array(self.codes()))
    }
}

#[derive(Debug)]
pub struct DateTimeEncoding;

pub const DATETIME_ENCODING: EncodingId = EncodingId::new("vortex.datetime");

impl Encoding for DateTimeEncoding {
    fn id(&self) -> &EncodingId {
        &DATETIME_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
