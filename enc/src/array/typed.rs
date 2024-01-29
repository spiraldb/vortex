use std::any::Any;
use std::borrow::Borrow;
use std::sync::{Arc, RwLock};

use arrow::datatypes::DataType;

use crate::array::stats::{Stats, StatsSet};
use crate::array::{Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;

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
}

impl Array for TypedArray {
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

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        let underlying = self.array.scalar_at(index)?;
        underlying.as_ref().cast(&self.dtype)
    }

    // TODO(robert): Have cast happen in enc space and not in arrow space
    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let datatype: DataType = self.dtype.borrow().into();
        Box::new(
            self.array.iter_arrow().map(move |arr| {
                arrow::compute::kernels::cast::cast(arr.as_ref(), &datatype).unwrap()
            }),
        )
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        Ok(Self::new(self.array.slice(start, stop)?, self.dtype.clone()).boxed())
    }

    fn nbytes(&self) -> usize {
        self.array.nbytes()
    }

    fn encoding(&self) -> EncodingRef {
        &TypedEncoding
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for TypedArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

#[derive(Debug)]
struct TypedEncoding;

pub const TYPED_ENCODING: EncodingId = EncodingId("enc.typed");

impl Encoding for TypedEncoding {
    fn id(&self) -> &EncodingId {
        &TYPED_ENCODING
    }
}

#[cfg(test)]
mod test {
    use std::iter;
    use std::ops::Deref;

    use arrow::array::cast::AsArray;
    use arrow::array::types::Time64MicrosecondType;
    use arrow::array::Time64MicrosecondArray;
    use itertools::Itertools;

    use crate::array::typed::TypedArray;
    use crate::array::Array;
    use crate::scalar::{LocalTimeScalar, PScalar, Scalar};
    use crate::types::{DType, TimeUnit};

    #[test]
    pub fn scalar() {
        let arr = TypedArray::new(
            vec![64_799_000_000_u64, 43_000_000_000].into(),
            DType::LocalTime(TimeUnit::Us),
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
            DType::LocalTime(TimeUnit::Us),
        );
        arr.iter_arrow()
            .zip_eq(iter::once(Box::new(Time64MicrosecondArray::from(vec![
                64_799_000_000i64,
                43_000_000_000,
            ]))))
            .for_each(|(enc, arrow)| {
                assert_eq!(
                    enc.as_primitive::<Time64MicrosecondType>().values().deref(),
                    arrow.values().deref()
                )
            });
    }
}
