use std::any::Any;
use std::sync::{Arc, RwLock};

use enc::array::{
    check_validity_buffer, Array, ArrayKind, ArrayRef, ArrowIterator, Encoding, EncodingId,
    EncodingRef,
};
use enc::compress::EncodingCompression;
use enc::dtype::DType;
use enc::error::{EncError, EncResult};
use enc::formatter::{ArrayDisplay, ArrayFormatter};
use enc::ptype::NativePType;
use enc::scalar::{NullableScalar, Scalar};
use enc::stats::{Stats, StatsSet};

use crate::compress::ffor_encode;

#[derive(Debug, Clone)]
pub struct FFORArray {
    encoded: ArrayRef,
    validity: Option<ArrayRef>,
    min_val: Box<dyn Scalar>,
    num_bits: u8,
    len: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl FFORArray {
    pub fn try_from_parts<T: NativePType>(
        encoded: ArrayRef,
        validity: Option<ArrayRef>,
        min_val: T,
        num_bits: u8,
        len: usize,
    ) -> EncResult<Self>
    where
        Box<dyn Scalar>: From<T>,
    {
        if !T::PTYPE.is_int() {
            return Err(EncError::InvalidPType(T::PTYPE));
        };
        let validity = validity.filter(|v| !v.is_empty());
        check_validity_buffer(validity.as_ref())?;

        let min_val: Box<dyn Scalar> = min_val.into();
        let min_val = if validity.is_some() {
            NullableScalar::some(min_val).boxed()
        } else {
            min_val
        };

        Ok(Self {
            encoded,
            validity,
            min_val,
            num_bits,
            len,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn encode(array: &dyn Array) -> EncResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(ffor_encode(p)),
            _ => Err(EncError::InvalidEncoding(array.encoding().id().clone())),
        }
    }

    pub fn encoded(&self) -> &dyn Array {
        self.encoded.as_ref()
    }

    pub fn min_val(&self) -> &dyn Scalar {
        self.min_val.as_ref()
    }

    pub fn num_bits(&self) -> u8 {
        self.num_bits
    }

    pub fn validity(&self) -> Option<&ArrayRef> {
        self.validity.as_ref()
    }
}

impl Array for FFORArray {
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
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.min_val.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, _index: usize) -> EncResult<Box<dyn Scalar>> {
        todo!()
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, _start: usize, _stop: usize) -> EncResult<ArrayRef> {
        unimplemented!("FFoRArray::slice")
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &FFoREncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.encoded.nbytes()
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for FFORArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for FFORArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln(format!(
            "min_val: {}, num_bits: {}",
            self.min_val(),
            self.num_bits()
        ))?;
        f.indent(|indent| indent.array(self.encoded()))
    }
}

#[derive(Debug)]
pub struct FFoREncoding;

pub const FFOR_ENCODING: EncodingId = EncodingId::new("enc.ffor");

impl Encoding for FFoREncoding {
    fn id(&self) -> &EncodingId {
        &FFOR_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }
}
