use std::fmt::{Display, Formatter};

use vortex_dtype::{match_each_native_ptype, DType};

use crate::bool::BoolScalar;
use crate::primitive::PrimitiveScalar;
use crate::Scalar;

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.dtype() {
            DType::Null => write!(f, "null"),
            DType::Bool(_) => match BoolScalar::try_from(self)
                .map_err(|err| {
                    debug_assert!(false, "failed to parse bool from scalar with DType Bool: {}", err);
                    std::fmt::Error
                })?
                .value()
            {
                None => write!(f, "null"),
                Some(b) => write!(f, "{}", b),
            },
            DType::Primitive(ptype, _) => match_each_native_ptype!(ptype, |$T| {
                match PrimitiveScalar::try_from(self).expect("primitive").typed_value::<$T>() {
                    None => write!(f, "null"),
                    Some(v) => write!(f, "{}", v),
                }
            }),
            DType::Utf8(_) => todo!(),
            DType::Binary(_) => todo!(),
            DType::Struct(..) => todo!(),
            DType::List(..) => todo!(),
            DType::Extension(..) => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Scalar;

    #[test]
    fn display() {
        let scalar = Scalar::from(false);
        assert_eq!(format!("{}", scalar), "false");
    }
}
