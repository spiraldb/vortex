use arrow2::array::BooleanArray as ArrowBooleanArray;
use arrow2::scalar::Scalar;

use crate::types::DType;

use super::{impl_array, Array, ArrowIterator};

#[derive(Clone)]
pub struct BoolArray {
    buffer: ArrowBooleanArray,
}

pub const KIND: &str = "enc.bool";

impl BoolArray {
    pub fn new(buffer: ArrowBooleanArray) -> Self {
        Self { buffer }
    }
}

impl Array for BoolArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &DType::Bool
    }

    #[inline]
    fn kind(&self) -> &str {
        KIND
    }

    fn scalar_at(&self, index: usize) -> Box<dyn Scalar> {
        arrow2::scalar::new_scalar(&self.buffer, index)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(std::iter::once(self.buffer.clone().boxed()))
    }
}
