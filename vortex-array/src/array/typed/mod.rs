use std::any::Any;
use std::sync::{Arc, RwLock};

use linkme::distributed_slice;

use crate::array::{Array, ArrayRef, Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::compress::EncodingCompression;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};

mod as_arrow;
mod compress;
mod compute;
mod serde;

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

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl StatsCompute for TypedArray {}

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
        f.child("untyped", self.untyped_array())
    }
}

#[cfg(test)]
mod test {
    use std::iter;

    use arrow_array::cast::AsArray;
    use arrow_array::types::Time64MicrosecondType;
    use arrow_array::Time64MicrosecondArray;
    use itertools::Itertools;

    use crate::array::typed::TypedArray;
    use crate::array::Array;
    use crate::composite_dtypes::{localtime, TimeUnit};
    use crate::compute::scalar_at::scalar_at;
    use crate::dtype::{IntWidth, Nullability};
    use crate::scalar::{CompositeScalar, PScalar, PrimitiveScalar};

    #[test]
    pub fn scalar() {
        let dtype = localtime(TimeUnit::Us, IntWidth::_64, Nullability::NonNullable);
        let arr = TypedArray::new(
            vec![64_799_000_000_u64, 43_000_000_000].into(),
            dtype.clone(),
        );
        assert_eq!(
            scalar_at(arr.as_ref(), 0).unwrap(),
            CompositeScalar::new(
                dtype.clone(),
                Box::new(PrimitiveScalar::some(PScalar::U64(64_799_000_000)).into()),
            )
            .into()
        );
        assert_eq!(
            scalar_at(arr.as_ref(), 1).unwrap(),
            CompositeScalar::new(
                dtype.clone(),
                Box::new(PrimitiveScalar::some(PScalar::U64(43_000_000_000)).into()),
            )
            .into()
        );
    }

    #[test]
    pub fn iter() {
        let dtype = localtime(TimeUnit::Us, IntWidth::_64, Nullability::NonNullable);

        let arr = TypedArray::new(vec![64_799_000_000_i64, 43_000_000_000].into(), dtype);
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
