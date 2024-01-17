use std::borrow::Borrow;

use arrow2::array::Array as ArrowArray;
use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;
use arrow2::datatypes::{DataType, PhysicalType};
use arrow2::with_match_primitive_without_interval_type;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq)]
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

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let datatype: DataType = self.dtype.borrow().into();
        Box::new(
            self.array
                .iter_arrow()
                .map(move |arr| match arr.data_type().to_physical_type() {
                    PhysicalType::Primitive(prim) => {
                        with_match_primitive_without_interval_type!(prim, |$T| {
                            let primitive_array: ArrowPrimitiveArray<$T> = arr
                                .as_any()
                                .downcast_ref::<ArrowPrimitiveArray<$T>>()
                                .unwrap()
                                .clone();
                            Box::new(primitive_array.to(datatype.clone())) as Box<dyn ArrowArray>
                        })
                    }
                    _ => panic!("Underlying typed array was not a primitive array"),
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
    use crate::array::primitive::PrimitiveArray;
    use crate::array::typed::TypedArray;
    use crate::array::{Array, ArrayEncoding};
    use crate::scalar::{LocalTimeScalar, PScalar, Scalar};
    use crate::types::{DType, TimeUnit};
    use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;
    use arrow2::datatypes::DataType;
    use itertools::Itertools;
    use std::iter;

    #[test]
    pub fn scalar() {
        let arr = TypedArray::new(
            Box::new(Array::Primitive(PrimitiveArray::from_vec(vec![
                64_799_000_000_u64,
                43_000_000_000,
            ]))),
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
            Box::new(Array::Primitive(PrimitiveArray::from_vec(vec![
                64_799_000_000_i64,
                43_000_000_000,
            ]))),
            DType::LocalTime(TimeUnit::Us),
        );
        arr.iter_arrow()
            .zip_eq(iter::once(Box::new(
                ArrowPrimitiveArray::from_vec(vec![64_799_000_000i64, 43_000_000_000])
                    .to(DataType::Time64(arrow2::datatypes::TimeUnit::Microsecond)),
            )))
            .for_each(|(enc, arrow)| {
                assert_eq!(
                    enc.as_any()
                        .downcast_ref::<ArrowPrimitiveArray<i64>>()
                        .unwrap()
                        .values()
                        .as_slice(),
                    arrow.values().as_slice()
                )
            });
    }
}
