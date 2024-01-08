use crate::array::constant::ConstantArray;
use crate::array::Array;
use crate::scalar::Scalar;

pub fn repeat(scalar: &dyn Scalar, n: usize) -> Array {
    ConstantArray::new(dyn_clone::clone_box(scalar), n).into()
}

#[cfg(test)]
mod test {
    use crate::array::ArrayEncoding;
    use crate::scalar::PrimitiveScalar;

    use super::*;

    #[test]
    fn test_repeat() {
        let array = repeat(&PrimitiveScalar::new(47), 100);
        assert_eq!(array.len(), 100);
    }
}
