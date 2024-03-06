use std::any::Any;
use std::sync::{Arc, RwLock};

use vortex::array::{
    check_validity_buffer, Array, ArrayKind, ArrayRef, ArrowIterator, Encoding, EncodingId,
    EncodingRef,
};
use vortex::compress::EncodingCompression;
use vortex::compute::scalar_at::scalar_at;
use vortex::compute::ArrayCompute;
use vortex::dtype::DType;
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::scalar::{Scalar, ScalarRef};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsCompute, StatsSet};

use crate::compress::ffor_encode;

#[derive(Debug, Clone)]
pub struct FFORArray {
    encoded: ArrayRef,
    validity: Option<ArrayRef>,
    patches: Option<ArrayRef>,
    min_val: ScalarRef,
    num_bits: u8,
    len: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl FFORArray {
    pub fn new(
        encoded: ArrayRef,
        validity: Option<ArrayRef>,
        patches: Option<ArrayRef>,
        min_val: ScalarRef,
        num_bits: u8,
        len: usize,
    ) -> Self {
        Self::try_new(encoded, validity, patches, min_val, num_bits, len).unwrap()
    }

    pub fn try_new(
        encoded: ArrayRef,
        validity: Option<ArrayRef>,
        patches: Option<ArrayRef>,
        min_val: ScalarRef,
        num_bits: u8,
        len: usize,
    ) -> VortexResult<Self> {
        let validity = validity.filter(|v| !v.is_empty());
        check_validity_buffer(validity.as_deref())?;

        if !matches!(min_val.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(min_val.dtype().clone()));
        }

        if validity.is_some() && !min_val.dtype().is_nullable() {
            return Err(VortexError::InvalidDType(min_val.dtype().clone()));
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

    pub fn encode(array: &dyn Array) -> VortexResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => Ok(ffor_encode(p).boxed()),
            _ => Err(VortexError::InvalidEncoding(array.encoding().id().clone())),
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
    pub fn validity(&self) -> Option<&dyn Array> {
        self.validity.as_deref()
    }

    #[inline]
    pub fn patches(&self) -> Option<&dyn Array> {
        self.patches.as_deref()
    }

    pub fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .map(|v| scalar_at(v, index).and_then(|v| v.try_into()).unwrap())
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

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
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

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl StatsCompute for FFORArray {}

impl ArrayCompute for FFORArray {}

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

impl FFoREncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.ffor");
}

impl Encoding for FFoREncoding {
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
