use arrow2::array::{Array, BooleanArray as ArrowBooleanArray};

use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;

use super::{ArrayEncoding, ArrowIterator};

#[derive(Debug, Clone, PartialEq)]
pub struct BoolArray {
    buffer: ArrowBooleanArray,
}

impl BoolArray {
    pub fn new(buffer: ArrowBooleanArray) -> Self {
        Self { buffer }
    }
}

impl ArrayEncoding for BoolArray {
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
        &DType::Bool
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        // TODO(ngates): this panics when index OOB
        Ok(arrow2::scalar::new_scalar(&self.buffer, index)
            .as_ref()
            .into())
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(std::iter::once(self.buffer.clone().boxed()))
    }
}
