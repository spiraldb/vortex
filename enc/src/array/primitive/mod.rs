use std::iter;

use arrow2::array::Array as ArrowArray;
use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;
use arrow2::types::NativeType;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::{DType, PType};

#[derive(Debug, Clone, PartialEq)]
pub struct PrimitiveArray {
    buffer: Box<dyn ArrowArray>,
    ptype: PType,
    dtype: DType,
}

impl PrimitiveArray {
    pub fn new<T: NativeType>(buffer: &ArrowPrimitiveArray<T>) -> Self {
        let ptype: PType = T::PRIMITIVE.try_into().unwrap();
        Self {
            buffer: buffer.to_boxed(),
            ptype,
            dtype: ptype.into(),
        }
    }

    pub fn from_vec<T: NativeType>(values: Vec<T>) -> Self {
        Self::new(&ArrowPrimitiveArray::from_vec(values))
    }
}

impl ArrayEncoding for PrimitiveArray {
    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        Ok(arrow2::scalar::new_scalar(self.buffer.as_ref(), index)
            .as_ref()
            .into())
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(iter::once(self.buffer.clone()))
    }

    fn slice(&self, offset: usize, length: usize) -> Array {
        let mut cloned = self.clone();
        cloned.buffer.slice(offset, length);
        Array::Primitive(cloned)
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Array {
        let mut cloned = self.clone();
        cloned.buffer.slice_unchecked(offset, length);
        Array::Primitive(cloned)
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
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(1));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(3));
    }

    #[test]
    fn slice() {
        let arr = PrimitiveArray::from_vec(vec![1, 2, 3, 4, 5]).slice(1, 3);
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(3));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(4));
    }
}
