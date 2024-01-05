use crate::types::{FloatWidth, IntWidth};

use super::*;

impl PartialEq for dyn Scalar + '_ {
    fn eq(&self, that: &dyn Scalar) -> bool {
        equal(self, that)
    }
}

// impl PartialEq<dyn Scalar> for Arc<dyn Scalar + '_> {
//     fn eq(&self, that: &dyn Scalar) -> bool {
//         equal(&**self, that)
//     }
// }

impl PartialEq<dyn Scalar> for Box<dyn Scalar + '_> {
    fn eq(&self, that: &dyn Scalar) -> bool {
        equal(&**self, that)
    }
}

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

    use DType::*;
    match lhs.dtype() {
        Bool => dyn_eq!(BoolScalar, lhs, rhs),
        Int(width) => match width {
            IntWidth::_8 => dyn_eq!(PrimitiveScalar<i8>, lhs, rhs),
            IntWidth::_16 => dyn_eq!(PrimitiveScalar<i16>, lhs, rhs),
            IntWidth::_32 => dyn_eq!(PrimitiveScalar<i32>, lhs, rhs),
            IntWidth::_64 => dyn_eq!(PrimitiveScalar<i64>, lhs, rhs),
            _ => unreachable!(),
        },
        UInt(width) => match width {
            IntWidth::_8 => dyn_eq!(PrimitiveScalar<u8>, lhs, rhs),
            IntWidth::_16 => dyn_eq!(PrimitiveScalar<u16>, lhs, rhs),
            IntWidth::_32 => dyn_eq!(PrimitiveScalar<u32>, lhs, rhs),
            IntWidth::_64 => dyn_eq!(PrimitiveScalar<u64>, lhs, rhs),
            _ => unreachable!(),
        },
        Float(width) => match width {
            // FloatWidth::_16 => dyn_eq!(PrimitiveScalar<f16>, lhs, rhs),
            FloatWidth::_32 => dyn_eq!(PrimitiveScalar<f32>, lhs, rhs),
            FloatWidth::_64 => dyn_eq!(PrimitiveScalar<f64>, lhs, rhs),
            _ => unreachable!(),
        },
        Struct(..) => dyn_eq!(StructScalar, lhs, rhs),
        Utf8 => dyn_eq!(Utf8Scalar, lhs, rhs),
        _ => todo!("Equal not yet implemented for {:?} {:?}", lhs, rhs),
    }
}
