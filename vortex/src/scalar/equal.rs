use std::sync::Arc;

use crate::scalar::localtime::LocalTimeScalar;
use crate::scalar::{
    BinaryScalar, BoolScalar, NullableScalar, PScalar, Scalar, StructScalar, Utf8Scalar,
};

impl PartialEq for dyn Scalar {
    fn eq(&self, that: &dyn Scalar) -> bool {
        equal(self, that)
    }
}

impl PartialEq<dyn Scalar> for Arc<dyn Scalar> {
    fn eq(&self, that: &dyn Scalar) -> bool {
        equal(&**self, that)
    }
}

impl PartialEq<dyn Scalar> for Box<dyn Scalar> {
    fn eq(&self, that: &dyn Scalar) -> bool {
        equal(self.as_ref(), that)
    }
}

impl Eq for dyn Scalar {}

macro_rules! dyn_eq {
    ($ty:ty, $lhs:expr, $rhs:expr) => {{
        let lhs = $lhs.as_any().downcast_ref::<$ty>().unwrap();
        let rhs = $rhs.as_any().downcast_ref::<$ty>().unwrap();
        lhs == rhs
    }};
}

fn equal(lhs: &dyn Scalar, rhs: &dyn Scalar) -> bool {
    if lhs.dtype() != rhs.dtype() {
        return false;
    }

    // If the dtypes are the same then both of the scalars are either nullable or plain scalar
    if let Some(ls) = lhs.as_any().downcast_ref::<NullableScalar>() {
        if let Some(rs) = rhs.as_any().downcast_ref::<NullableScalar>() {
            return dyn_eq!(NullableScalar, ls, rs);
        } else {
            unreachable!("DTypes were equal, but only one was nullable")
        }
    }

    use crate::dtype::DType::*;
    match lhs.dtype() {
        Bool(_) => dyn_eq!(BoolScalar, lhs, rhs),
        Int(_, _, _) => dyn_eq!(PScalar, lhs, rhs),
        Float(_, _) => dyn_eq!(PScalar, lhs, rhs),
        Struct(..) => dyn_eq!(StructScalar, lhs, rhs),
        Utf8(_) => dyn_eq!(Utf8Scalar, lhs, rhs),
        Binary(_) => dyn_eq!(BinaryScalar, lhs, rhs),
        LocalTime(_, _) => dyn_eq!(LocalTimeScalar, lhs, rhs),
        _ => todo!("Equal not yet implemented for {:?} {:?}", lhs, rhs),
    }
}
