use std::borrow::Borrow;

use arrow::datatypes::DataType;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone)]
pub struct TypedArray {
    array: Box<Array>,
    dtype: DType,
}

impl TypedArray {
    pub fn new(array: Box<Array>, dtype: DType) -> Self {
        Self { array, dtype }
    }
}

impl ArrayEncoding for TypedArray {
    fn len(&self) -> usize {
        self.array.len()
    }

    fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    fn dtype(&self) -> DType {
        self.dtype.clone()
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

    fn slice(&self, start: usize, stop: usize) -> EncResult<Array> {
        Ok(Array::Typed(Self::new(
            Box::new(self.array.as_ref().slice(start, stop)?),
            self.dtype.clone(),
        )))
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
    use crate::array::ArrayEncoding;
    use crate::scalar::{LocalTimeScalar, PScalar, Scalar};
    use crate::types::{DType, TimeUnit};

    #[test]
    pub fn scalar() {
        let arr = TypedArray::new(
            Box::new(vec![64_799_000_000_u64, 43_000_000_000].into()),
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
            Box::new(vec![64_799_000_000_i64, 43_000_000_000].into()),
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
