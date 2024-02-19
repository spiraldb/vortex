use std::any::Any;
use std::sync::{Arc, RwLock};

use enc::array::primitive::PrimitiveArray;
use enc::array::{
    check_validity_buffer, Array, ArrayKind, ArrayRef, ArrowIterator, Encoding, EncodingId,
    EncodingRef,
};
use enc::compress::EncodingCompression;
use enc::dtype::DType;
use enc::error::{EncError, EncResult};
use enc::formatter::{ArrayDisplay, ArrayFormatter};
use enc::match_each_integer_ptype;
use enc::scalar::{NullableScalar, Scalar};
use enc::stats::{Stats, StatsSet};

use crate::compress::ffor_encode;

#[derive(Debug, Clone)]
pub struct FFORArray {
    encoded: ArrayRef,
    validity: Option<ArrayRef>,
    patches: Option<ArrayRef>,
    min_val: Box<dyn Scalar>,
    num_bits: u8,
    len: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl FFORArray {
    pub fn new(
        encoded: ArrayRef,
        validity: Option<ArrayRef>,
        patches: Option<ArrayRef>,
        min_val: Box<dyn Scalar>,
        num_bits: u8,
        len: usize,
    ) -> Self {
        Self::try_new(encoded, validity, patches, min_val, num_bits, len).unwrap()
    }

    pub fn try_new(
        encoded: ArrayRef,
        validity: Option<ArrayRef>,
        patches: Option<ArrayRef>,
        min_val: Box<dyn Scalar>,
        num_bits: u8,
        len: usize,
    ) -> EncResult<Self> {
        let validity = validity.filter(|v| !v.is_empty());
        check_validity_buffer(validity.as_ref())?;

        if !matches!(min_val.dtype(), DType::Int(_, _, _)) {
            return Err(EncError::InvalidDType(min_val.dtype().clone()));
        }

        if validity.is_some() && !min_val.dtype().is_nullable() {
            return Err(EncError::InvalidDType(min_val.dtype().clone()));
        }

        Ok(Self {
            encoded,
            validity,
            patches,
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

    #[inline]
    pub fn encoded(&self) -> &dyn Array {
        self.encoded.as_ref()
    }

    #[inline]
    pub fn min_val(&self) -> &dyn Scalar {
        self.min_val.as_ref()
    }

    #[inline]
    pub fn num_bits(&self) -> u8 {
        self.num_bits
    }

    #[inline]
    pub fn validity(&self) -> Option<&ArrayRef> {
        self.validity.as_ref()
    }

    #[inline]
    pub fn patches(&self) -> Option<&ArrayRef> {
        self.patches.as_ref()
    }

    pub fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .map(|v| v.scalar_at(index).and_then(|v| v.try_into()).unwrap())
            .unwrap_or(true)
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

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        if !self.is_valid(index) {
            return Ok(NullableScalar::none(self.dtype().clone()).boxed());
        }

        if let Some(patch) = self
            .patches()
            .and_then(|p| p.scalar_at(index).ok())
            .and_then(|p| p.into_nonnull())
        {
            return Ok(patch);
        }

        let Some(parray) = self.encoded().as_any().downcast_ref::<PrimitiveArray>() else {
            return Err(EncError::InvalidEncoding(
                self.encoded().encoding().id().clone(),
            ));
        };

        if let Ok(ptype) = self.dtype().try_into() {
            match_each_integer_ptype!(ptype, |$T| {
            return Ok(codecz::ffor::decode_single::<$T>(
                parray.buffer().as_slice(),
                self.len,
                self.num_bits,
                self.min_val().try_into().unwrap(),
                index,
            )
            .unwrap()
            .into());
            })
        } else {
            return Err(EncError::InvalidDType(self.dtype().clone()));
        }
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
        self.encoded().nbytes() + self.patches().map(|p| p.nbytes()).unwrap_or(0)
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
        if let Some(p) = self.patches() {
            f.writeln("patches:")?;
            f.indent(|indent| indent.array(p.as_ref()))?;
        }
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
