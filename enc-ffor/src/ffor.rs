use std::any::Any;
use std::sync::{Arc, RwLock};

use codecz::ffor::SupportsFFoR;
use enc::array::{Array, ArrayKind, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};
use enc::compress::{ArrayCompression, EncodingCompression};
use enc::dtype::DType;
use enc::error::{EncError, EncResult};
use enc::formatter::{ArrayDisplay, ArrayFormatter};
use enc::ptype::NativePType;
use enc::scalar::Scalar;
use enc::stats::{Stats, StatsSet};

use crate::compress::ffor_encode;

#[derive(Debug, Clone)]
pub struct FFORArray {
    encoded: ArrayRef,
    min_val: Box<dyn Scalar>,
    num_bits: u8,
    len: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl FFORArray {
    pub fn try_from_parts<T: SupportsFFoR + NativePType>(
        encoded: ArrayRef,
        min_val: T,
        num_bits: u8,
        len: usize,
    ) -> EncResult<Self>
    where
        Box<dyn Scalar>: From<T>,
    {
        let min_val: Box<dyn Scalar> = min_val.into();
        if !T::PTYPE.is_int() {
            return Err(EncError::InvalidPType(T::PTYPE));
        };

        Ok(Self {
            encoded,
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

    pub fn min_val(&self) -> Box<dyn Scalar> {
        self.min_val.clone()
    }

    pub fn num_bits(&self) -> u8 {
        self.num_bits
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

    fn compression(&self) -> Option<&dyn ArrayCompression> {
        // FFOR and other bitpacking algorithms are essentially the "terminal"
        // lightweight encodings for integers, as the output is essentially an array
        // of opaque bytes. At that point, the only available schemes are general-purpose
        // compression algorithms, which we would apply at the file level instead (if at all)
        None
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for FFORArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for FFORArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln(format!("min_val: {}, len: {}", &self.min_val, self.len))?;
        f.indent(|indent| indent.array(self.encoded.as_ref()))
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
