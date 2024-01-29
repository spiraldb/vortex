use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayKind, ArrayRef};
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;

// TODO(ngates): convert this to arithmetic operations with macro over the kernel.
pub fn add<T: AsRef<dyn Array>>(lhs: &T, rhs: &T) -> EncResult<ArrayRef> {
    // Check that the arrays are the same length.
    let length = lhs.as_ref().len();
    if rhs.as_ref().len() != length {
        return Err(EncError::LengthMismatch);
    }

    match (lhs.as_ref().kind(), rhs.as_ref().kind()) {
        (ArrayKind::Constant(lhs), ArrayKind::Constant(rhs)) => {
            Ok(ConstantArray::new(add_scalars(lhs.value(), rhs.value())?, length).boxed())
        }
        (ArrayKind::Constant(lhs), _) => add_scalar(rhs, lhs.value()),
        (_, ArrayKind::Constant(rhs)) => add_scalar(lhs, rhs.value()),
        _ => {
            todo!("Implement default addition")
        }
    }
}

pub fn add_scalar<T: AsRef<dyn Array>>(lhs: &T, rhs: &dyn Scalar) -> EncResult<ArrayRef> {
    match lhs.as_ref().kind() {
        ArrayKind::Constant(lhs) => {
            Ok(ConstantArray::new(add_scalars(lhs.value(), rhs)?, lhs.as_ref().len()).boxed())
        }
        _ => {
            todo!("Implement default addition")
        }
    }
}

pub fn add_scalars(_lhs: &dyn Scalar, _rhs: &dyn Scalar) -> EncResult<Box<dyn Scalar>> {
    // Might need to improve this implementation...
    Ok(24.into())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_add() {
        let lhs = ConstantArray::new(47.into(), 100);
        let rhs = ConstantArray::new(47.into(), 100);
        let result = add(&lhs, &rhs).unwrap();
        assert_eq!(result.len(), 100);
        // assert_eq!(result.scalar_at(0), 94);
    }
}
