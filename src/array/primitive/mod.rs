mod mutable;

use crate::array::{impl_array, Array};
use crate::types::PrimitiveType;
use crate::types::{DType, PType};

use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;

#[derive(Clone)]
pub struct PrimitiveArray<T: PrimitiveType> {
    buffer: ArrowPrimitiveArray<T::ArrowType>,
    ptype: PType,
    dtype: DType,
}

pub const KIND: &str = "enc.primitive";

impl<T: PrimitiveType> PrimitiveArray<T> {
    pub fn new(buffer: ArrowPrimitiveArray<T::ArrowType>) -> Self {
        Self {
            buffer,
            ptype: T::PTYPE,
            dtype: T::PTYPE.into(),
        }
    }
}

impl<T: PrimitiveType> Array for PrimitiveArray<T> {
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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::types::IntWidth;
    #[test]
    fn from_arrow() {
        let arr = PrimitiveArray::<i32>::new(ArrowPrimitiveArray::from_vec(vec![1, 2, 3]));
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.ptype, PType::I32);
        assert_eq!(arr.dtype, DType::Int(IntWidth::_32));
        assert_eq!(arr.buffer.values().as_slice(), &[1, 2, 3]);
    }
}
