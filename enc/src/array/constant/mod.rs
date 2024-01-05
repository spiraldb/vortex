use crate::array::{impl_array, Array, ArrowIterator};
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Clone)]
pub struct ConstantArray {
    scalar: Box<dyn Scalar>,
    length: usize,
}

pub const KIND: &str = "enc.constant";

impl ConstantArray {
    pub fn new(scalar: &dyn Scalar, length: usize) -> Self {
        Self {
            scalar: dyn_clone::clone_box(scalar),
            length,
        }
    }
}

impl Array for ConstantArray {
    impl_array!();

    fn len(&self) -> usize {
        self.length
    }

    fn dtype(&self) -> &DType {
        self.scalar.dtype()
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
        let arrow_scalar: Box<dyn arrow2::scalar::Scalar> =
            self.scalar.as_ref().try_into().unwrap();
        Box::new(std::iter::once(
            crate::arrow::compute::repeat(arrow_scalar.as_ref(), self.length).unwrap(),
        ))
    }
}
