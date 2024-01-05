use std::iter;

use arrow2::array::Array as ArrowArray;
use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;
use arrow2::datatypes::PhysicalType;
use arrow2::types::NativeType;
use arrow2::types::PrimitiveType as ArrowPrimitiveType;

use crate::array::{impl_array, Array, ArrowIterator};
use crate::scalar::Scalar;
use crate::types::{DType, PType};

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

    pub fn from_vec<T: NativeType>(values: Vec<T>) -> Self {
        Self::new(&ArrowPrimitiveArray::from_vec(values))
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
        return arrow2::scalar::new_scalar(self.buffer.as_ref(), index)
            .as_ref()
            .try_into()
            .unwrap();
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
        let arr = PrimitiveArray::from_vec(vec![1, 2, 3]);
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.ptype, PType::I32);
        assert_eq!(arr.dtype, DType::Int(IntWidth::_32));

        // Ensure we can fetch the scalar at the given index.
        assert_eq!(arr.scalar_at(0).try_into(), Ok(1));
        assert_eq!(arr.scalar_at(1).try_into(), Ok(2));
        assert_eq!(arr.scalar_at(2).try_into(), Ok(3));
    }
}
