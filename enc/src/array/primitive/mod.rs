use std::iter;

use arrow2::array::Array as ArrowArray;
use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;
use arrow2::datatypes::PhysicalType;
use arrow2::scalar::PrimitiveScalar as ArrowPrimitiveScalar;
use arrow2::types::NativeType;
use arrow2::types::PrimitiveType as ArrowPrimitiveType;
use arrow2::with_match_primitive_without_interval_type;

use crate::array::{impl_array, Array, ArrowIterator};
use crate::types::{DType, PType};
use crate::Scalar;

#[derive(Clone)]
pub struct PrimitiveArray {
    buffer: Box<dyn ArrowArray>,
    ptype: PType,
    dtype: DType,
}

pub const KIND: &str = "enc.primitive";

impl PrimitiveArray {
    pub fn new(buffer: &dyn ArrowArray) -> Self {
        let ptype: PType = buffer.data_type().try_into().unwrap();
        Self {
            buffer: buffer.to_boxed(),
            ptype,
            dtype: ptype.into(),
        }
    }

    pub fn unchecked_scalar_at<T: NativeType>(&self, index: usize) -> Option<T> {
        // Utility function for extracting a scalar and casting it into the native type.
        // Panics if the type is incorrect.
        // TODO(ngates): add a cast_scalar_at which is useful when we know our array must
        //  be an integer, but we don't care which integer width it is.
        self.buffer
            .as_any()
            .downcast_ref::<ArrowPrimitiveArray<T>>()
            .unwrap()
            .get(index)
    }

    fn primitive_type(&self) -> ArrowPrimitiveType {
        match self.buffer.data_type().to_physical_type() {
            PhysicalType::Primitive(primitive) => primitive,
            _ => panic!("Not a primitive type"),
        }
    }
}

impl Array for PrimitiveArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn kind(&self) -> &str {
        KIND
    }

    fn scalar_at(&self, index: usize) -> Box<dyn Scalar> {
        with_match_primitive_without_interval_type!(self.primitive_type(), |$T| {
            let value: Option<$T> = self.buffer
                .as_any()
                .downcast_ref::<ArrowPrimitiveArray<$T>>()
                .unwrap()
                .get(index);
            dyn_clone::clone_box(&ArrowPrimitiveScalar::from(value))
        })
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(iter::once(self.buffer.clone()))
    }
}

#[cfg(test)]
mod test {
    use crate::types::IntWidth;

    use super::*;

    #[test]
    fn from_arrow() {
        let arr = PrimitiveArray::new(&ArrowPrimitiveArray::<i32>::from_vec(vec![1, 2, 3]));
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.ptype, PType::I32);
        assert_eq!(arr.dtype, DType::Int(IntWidth::_32));

        // Ensure we can fetch the scalar at the given index.
        assert_eq!(
            arr.scalar_at(0).as_ref(),
            &ArrowPrimitiveScalar::from(Some(1)) as &dyn Scalar
        );

        assert_eq!(arr.unchecked_scalar_at(1), Some(2));
        assert_eq!(arr.unchecked_scalar_at(2), Some(3));
    }
}
