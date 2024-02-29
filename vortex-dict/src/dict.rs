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
use vortex::dtype::{DType, Signedness};
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::scalar::Scalar;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};

#[derive(Debug, Clone)]
pub struct DictArray {
    codes: ArrayRef,
    dict: ArrayRef,
    stats: Arc<RwLock<StatsSet>>,
}

impl DictArray {
    pub fn new(codes: ArrayRef, dict: ArrayRef) -> Self {
        Self::try_new(codes, dict).unwrap()
    }

    pub fn try_new(codes: ArrayRef, dict: ArrayRef) -> VortexResult<Self> {
        if !matches!(codes.dtype(), DType::Int(_, Signedness::Unsigned, _)) {
            return Err(VortexError::InvalidDType(codes.dtype().clone()));
        }
        Ok(Self {
            codes,
            dict,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn dict(&self) -> &dyn Array {
        self.dict.as_ref()
    }

    #[inline]
    pub fn codes(&self) -> &dyn Array {
        self.codes.as_ref()
    }
}

impl Array for DictArray {
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
        self.codes.len()
    }

    fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    fn dtype(&self) -> &DType {
        self.dict.dtype()
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
        &DictEncoding
    }

    fn nbytes(&self) -> usize {
        self.codes().nbytes() + self.dict().nbytes()
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for DictArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for DictArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("dict:")?;
        f.indent(|indent| indent.array(self.dict()))?;
        f.writeln("codes:")?;
        f.indent(|indent| indent.array(self.codes()))
    }
}

#[derive(Debug)]
pub struct DictEncoding;

pub const DICT_ENCODING: EncodingId = EncodingId::new("vortex.dict");

impl Encoding for DictEncoding {
    fn id(&self) -> &EncodingId {
        &DICT_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
