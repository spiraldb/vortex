use crate::array::constant::ConstantArray;
use crate::array::Array;
use crate::scalar::Scalar;

pub fn repeat(scalar: &dyn Scalar, n: usize) -> Box<dyn Array> {
    ConstantArray::new(scalar, n).boxed()
}

#[cfg(test)]
mod test {
    use crate::scalar::PrimitiveScalar;

    use super::*;

    #[test]
    fn test_repeat() {
        let array = repeat(&PrimitiveScalar::new(47), 100);
        assert_eq!(array.len(), 100);
    }
}
