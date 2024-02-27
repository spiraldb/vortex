use std::any::Any;
use std::sync::{Arc, RwLock};

use codecz::alp;
pub use codecz::alp::ALPExponents;
use vortex::array::{Array, ArrayKind, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};
use vortex::compress::EncodingCompression;
use vortex::dtype::{DType, FloatWidth, IntWidth, Signedness};
use vortex::error::{VortexError, EncResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::scalar::{NullableScalar, Scalar};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsSet};

use crate::compress::alp_encode;

#[derive(Debug, Clone)]
pub struct ALPArray {
    encoded: ArrayRef,
    exponents: ALPExponents,
    patches: Option<ArrayRef>,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl ALPArray {
    pub fn new(encoded: ArrayRef, exponents: ALPExponents, patches: Option<ArrayRef>) -> Self {
        Self::try_new(encoded, exponents, patches).unwrap()
    }

    pub fn try_new(
        encoded: ArrayRef,
        exponents: ALPExponents,
        patches: Option<ArrayRef>,
    ) -> EncResult<Self> {
        let dtype = match encoded.dtype() {
            d @ DType::Int(width, Signedness::Signed, nullability) => match width {
                IntWidth::_32 => DType::Float(32.into(), *nullability),
                IntWidth::_64 => DType::Float(64.into(), *nullability),
                _ => return Err(VortexError::InvalidDType(d.clone())),
            },
            d => return Err(VortexError::InvalidDType(d.clone())),
        };
        Ok(Self {
            encoded,
            exponents,
            patches,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn encode(array: &dyn Array) -> EncResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(alp_encode(p).boxed()),
            _ => Err(VortexError::InvalidEncoding(array.encoding().id().clone())),
        }
    }

    pub fn encoded(&self) -> &dyn Array {
        self.encoded.as_ref()
    }

    pub fn exponents(&self) -> ALPExponents {
        self.exponents
    }

    pub fn patches(&self) -> Option<&ArrayRef> {
        self.patches.as_ref()
    }
}

impl Array for ALPArray {
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

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        if let Some(patch) = self
            .patches()
            .and_then(|p| p.scalar_at(index).ok())
            .and_then(|p| p.into_nonnull())
        {
            return Ok(patch);
        }

        let Some(encoded_val) = self.encoded.scalar_at(index)?.into_nonnull() else {
            return Ok(NullableScalar::none(self.dtype().clone()).boxed());
        };
        match self.dtype {
            DType::Float(FloatWidth::_32, _) => {
                let encoded_val: i32 = encoded_val.try_into().unwrap();
                Ok(alp::decode_single::<f32>(encoded_val, self.exponents)
                    .unwrap()
                    .into())
            }

            DType::Float(FloatWidth::_64, _) => {
                let encoded_val: i64 = encoded_val.try_into().unwrap();
                Ok(alp::decode_single::<f64>(encoded_val, self.exponents)
                    .unwrap()
                    .into())
            }

            _ => unreachable!(),
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        Ok(Self::try_new(
            self.encoded().slice(start, stop)?,
            self.exponents(),
            self.patches().map(|p| p.slice(start, stop)).transpose()?,
        )?
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ALPEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.encoded().nbytes() + self.patches().map(|p| p.nbytes()).unwrap_or(0)
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for ALPArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for ALPArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln(format!("exponents: {}", self.exponents()))?;
        if let Some(p) = self.patches() {
            f.writeln("patches:")?;
            f.indent(|indent| indent.array(p.as_ref()))?;
        }
        f.indent(|indent| indent.array(self.encoded()))
    }
}

#[derive(Debug)]
pub struct ALPEncoding;

pub const ALP_ENCODING: EncodingId = EncodingId::new("vortex.alp");

impl Encoding for ALPEncoding {
    fn id(&self) -> &EncodingId {
        &ALP_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
