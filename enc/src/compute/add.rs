use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayEncoding};
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::PrimitiveType;

// TODO(ngates): convert this to arithmetic operations with macro over the kernel.
pub fn add(lhs: &Array, rhs: &Array) -> EncResult<Array> {
    // Check that the arrays are the same length.
    let length = lhs.len();
    if rhs.len() != length {
        return Err(EncError::LengthMismatch);
    }

    use Array::*;
    match (lhs, rhs) {
        (Constant(lhs), Constant(rhs)) => {
            Ok(ConstantArray::new(add_scalars(lhs.value(), rhs.value())?, length).into())
        }
        (Constant(lhs), _) => add_scalar(rhs, lhs.value()),
        (_, Constant(rhs)) => add_scalar(lhs, rhs.value()),
        _ => {
            todo!("Implement default addition")
        }
    }
}

pub fn add_scalar(lhs: &Array, rhs: &dyn Scalar) -> EncResult<Array> {
    use Array::*;
    match lhs {
        Constant(lhs) => Ok(ConstantArray::new(add_scalars(lhs.value(), rhs)?, lhs.len()).into()),
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
        let lhs = ConstantArray::new(47.into(), 100).into();
        let rhs = ConstantArray::new(47.into(), 100).into();
        let result = add(&lhs, &rhs).unwrap();
        assert_eq!(result.len(), 100);
        // assert_eq!(result.scalar_at(0), 94);
    }
}
