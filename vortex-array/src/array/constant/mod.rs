use std::any::Any;
use std::sync::{Arc, RwLock};

use linkme::distributed_slice;

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::{
    check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef,
    ENCODINGS,
};
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::match_each_native_ptype;
use crate::scalar::{PScalar, Scalar};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stat, Stats, StatsSet};

mod compute;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct ConstantArray {
    scalar: Scalar,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl ConstantArray {
    pub fn new(scalar: Scalar, length: usize) -> Self {
        let stats = StatsSet::from(
            [
                (Stat::Max, scalar.clone()),
                (Stat::Min, scalar.clone()),
                (Stat::IsConstant, true.into()),
                (Stat::IsSorted, true.into()),
                (Stat::RunCount, 1.into()),
            ]
            .into(),
        );
        Self {
            scalar,
            length,
            stats: Arc::new(RwLock::new(stats)),
        }
    }

    pub fn scalar(&self) -> &Scalar {
        &self.scalar
    }
}

impl Array for ConstantArray {
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
        self.length
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.scalar.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let plain_array = match self.scalar() {
            Scalar::Bool(b) => {
                if let Some(bv) = b.value() {
                    BoolArray::from(vec![bv; self.len()]).boxed()
                } else {
                    BoolArray::null(self.len()).boxed()
                }
            }
            Scalar::Primitive(p) => {
                if let Some(ps) = p.value() {
                    match ps {
                        PScalar::U8(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                        PScalar::U16(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                        PScalar::U32(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                        PScalar::U64(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                        PScalar::I8(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                        PScalar::I16(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                        PScalar::I32(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                        PScalar::I64(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                        PScalar::F16(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                        PScalar::F32(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                        PScalar::F64(p) => PrimitiveArray::from_value(p, self.len()).boxed(),
                    }
                } else {
                    match_each_native_ptype!(p.ptype(), |$P| {
                        PrimitiveArray::null::<$P>(self.len()).boxed()
                    })
                }
            }
            _ => panic!("Unsupported scalar type {}", self.dtype()),
        };
        plain_array.iter_arrow()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(ConstantArray::new(self.scalar.clone(), stop - start).boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ConstantEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.scalar.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for ConstantArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for ConstantArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("scalar", self.scalar())
    }
}

#[derive(Debug)]
pub struct ConstantEncoding;

impl ConstantEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.constant");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_CONSTANT: EncodingRef = &ConstantEncoding;

impl Encoding for ConstantEncoding {
    fn id(&self) -> &EncodingId {
        &Self::ID
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
