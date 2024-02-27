use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayRef};
use crate::scalar::Scalar;

pub fn repeat(scalar: &dyn Scalar, n: usize) -> ArrayRef {
    ConstantArray::new(dyn_clone::clone_box(scalar), n).boxed()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_repeat() {
        let scalar: Box<dyn Scalar> = 47.into();
        let array = repeat(scalar.as_ref(), 100);
        assert_eq!(array.len(), 100);
    }
}
