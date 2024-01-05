use arrow2::array::{Array as ArrowArray, BooleanArray as ArrowBooleanArray};

use crate::scalar::Scalar;
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
            .as_ref()
            .try_into()
            .unwrap()
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(std::iter::once(self.buffer.clone().boxed()))
    }
}
