use crate::array::constant::ConstantArray;
use crate::array::Array;
use crate::Scalar;

pub fn repeat(scalar: &dyn Scalar, n: usize) -> Result<Box<dyn Array>, ()> {
    Ok(ConstantArray::new(scalar, n).boxed())
}

#[cfg(test)]
mod test {
    use arrow2::scalar;

    use super::*;

    #[test]
    fn test_repeat() {
        let scalar = scalar::PrimitiveScalar::from(Some::<u64>(47));
        let array = repeat(&scalar, 100).unwrap();
        assert_eq!(array.len(), 100);
    }
}
