use std::any::Any;
use std::sync::{Arc, RwLock};

use arrow::datatypes::DataType;
use linkme::distributed_slice;

use crate::array::{Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::compress::EncodingCompression;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::Scalar;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsSet};

mod compress;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct TypedArray {
    array: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl TypedArray {
    pub fn new(array: ArrayRef, dtype: DType) -> Self {
        Self {
            array,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    /// Possibly wrap an array in a TypedArray if the dtype is different
    pub fn maybe_wrap(array: ArrayRef, dtype: &DType) -> ArrayRef {
        if array.dtype() == dtype {
            array
        } else {
            // Should we check the DType is compatible...?
            Self::new(array, dtype.clone()).boxed()
        }
    }

    #[inline]
    pub fn untyped_array(&self) -> &dyn Array {
        self.array.as_ref()
    }
}

impl Array for TypedArray {
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
        self.array.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        let underlying = self.array.scalar_at(index)?;
        underlying.as_ref().cast(self.dtype())
    }

    // TODO(robert): Have cast happen in enc space and not in arrow space
    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let datatype: DataType = self.dtype().into();
        Box::new(
            self.array.iter_arrow().map(move |arr| {
                arrow::compute::kernels::cast::cast(arr.as_ref(), &datatype).unwrap()
            }),
        )
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(Self::new(self.array.slice(start, stop)?, self.dtype.clone()).boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &TypedEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.array.nbytes()
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for TypedArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

#[derive(Debug)]
pub struct TypedEncoding;

impl TypedEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.typed");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_TYPED: EncodingRef = &TypedEncoding;

impl Encoding for TypedEncoding {
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

impl ArrayDisplay for TypedArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.indent(|indented| indented.array(self.untyped_array()))
    }
}

#[cfg(test)]
mod test {
    use std::iter;

    use arrow::array::cast::AsArray;
    use arrow::array::types::Time64MicrosecondType;
    use arrow::array::Time64MicrosecondArray;
    use itertools::Itertools;

    use crate::array::typed::TypedArray;
    use crate::array::Array;
    use crate::dtype::{DType, Nullability, TimeUnit};
    use crate::scalar::{LocalTimeScalar, PScalar, Scalar};

    #[test]
    pub fn scalar() {
        let arr = TypedArray::new(
            vec![64_799_000_000_u64, 43_000_000_000].into(),
            DType::LocalTime(TimeUnit::Us, Nullability::NonNullable),
        );
        assert_eq!(
            arr.scalar_at(0).unwrap().as_ref(),
            &LocalTimeScalar::new(PScalar::U64(64_799_000_000), TimeUnit::Us) as &dyn Scalar
        );
        assert_eq!(
            arr.scalar_at(1).unwrap().as_ref(),
            &LocalTimeScalar::new(PScalar::U64(43_000_000_000), TimeUnit::Us) as &dyn Scalar
        );
    }

    #[test]
    pub fn iter() {
        let arr = TypedArray::new(
            vec![64_799_000_000_i64, 43_000_000_000].into(),
            DType::LocalTime(TimeUnit::Us, Nullability::NonNullable),
        );
        arr.iter_arrow()
            .zip_eq(iter::once(Box::new(Time64MicrosecondArray::from(vec![
                64_799_000_000i64,
                43_000_000_000,
            ]))))
            .for_each(|(enc, arrow)| {
                assert_eq!(
                    *enc.as_primitive::<Time64MicrosecondType>().values(),
                    *arrow.values()
                )
            });
    }
}
