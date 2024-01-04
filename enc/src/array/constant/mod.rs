use crate::array::{impl_array, Array, ArrowIterator};
use crate::types::DType;
use crate::Scalar;

#[derive(Clone)]
pub struct ConstantArray {
    scalar: Box<dyn Scalar>,
    length: usize,
    dtype: DType,
}

pub const KIND: &str = "enc.constant";

impl ConstantArray {
    pub fn new(scalar: &dyn Scalar, length: usize) -> Self {
        let dtype = scalar.data_type().try_into().unwrap();
        Self {
            scalar: dyn_clone::clone_box(scalar),
            length,
            dtype,
        }
    }
}

impl Array for ConstantArray {
    impl_array!();

    fn len(&self) -> usize {
        self.length
    }

    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn kind(&self) -> &str {
        KIND
    }

    // TODO(robert): Return Result
    fn scalar_at(&self, index: usize) -> Box<dyn Scalar> {
        if index >= self.length {
            panic!("TODO(robert): return result")
        }
        self.scalar.clone()
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(std::iter::once(
            crate::arrow::compute::repeat(self.scalar.as_ref(), self.length).unwrap(),
        ))
    }
}
