use crate::array::constant::ConstantArray;
use crate::array::Array;
use crate::scalar::Scalar;

pub fn repeat(scalar: &dyn Scalar, n: usize) -> Result<Box<dyn Array>, ()> {
    Ok(ConstantArray::new(scalar, n).boxed())
}

#[cfg(test)]
mod test {
    use crate::scalar::PrimitiveScalar;

    use super::*;

    #[test]
    fn test_repeat() {
        let array = repeat(&PrimitiveScalar::new(47), 100).unwrap();
        assert_eq!(array.len(), 100);
    }
}
