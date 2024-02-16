use std::any::Any;
use std::sync::{Arc, RwLock};

use enc::array::{
    check_index_bounds, check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId,
};
use enc::compress::EncodingCompression;
use enc::dtype::{DType, Signedness};
use enc::error::{EncError, EncResult};
use enc::formatter::{ArrayDisplay, ArrayFormatter};
use enc::scalar::Scalar;
use enc::stats::{Stats, StatsSet};

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

    pub fn try_new(codes: ArrayRef, dict: ArrayRef) -> EncResult<Self> {
        if !matches!(codes.dtype(), DType::Int(_, Signedness::Unsigned, _)) {
            return Err(EncError::InvalidDType(codes.dtype().clone()));
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

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        check_index_bounds(self, index)?;
        let dict_index: usize = self.codes.scalar_at(index)?.try_into()?;
        self.dict.scalar_at(dict_index)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    // TODO(robert): Add function to trim the dictionary
    fn slice(&self, _start: usize, _stop: usize) -> EncResult<ArrayRef> {
        check_slice_bounds(self, _start, _stop)?;
        Ok(Self::new(self.codes().slice(_start, _stop)?, self.dict.clone()).boxed())
    }

    fn encoding(&self) -> &'static dyn Encoding {
        &DictEncoding
    }

    fn nbytes(&self) -> usize {
        self.codes().nbytes() + self.dict().nbytes()
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

pub const DICT_ENCODING: EncodingId = EncodingId::new("enc.dict");

impl Encoding for DictEncoding {
    fn id(&self) -> &EncodingId {
        &DICT_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }
}
