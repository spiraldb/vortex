use std::fmt::{Display, Formatter};

use itertools::Itertools;
use vortex_dtype::{match_each_native_ptype, DType};

use crate::binary::BinaryScalar;
use crate::bool::BoolScalar;
use crate::primitive::PrimitiveScalar;
use crate::utf8::Utf8Scalar;
use crate::Scalar;

impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.dtype() {
            DType::Null => write!(f, "null"),
            DType::Bool(_) => match BoolScalar::try_from(self)
                .map_err(|_| std::fmt::Error)?
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
            DType::Utf8(_) => {
                match Utf8Scalar::try_from(self)
                    .map_err(|_| std::fmt::Error)?
                    .value()
                {
                    None => write!(f, "null"),
                    Some(bs) => write!(f, "{}", bs.as_str()),
                }
            }
            DType::Binary(_) => {
                match BinaryScalar::try_from(self)
                    .map_err(|_| std::fmt::Error)?
                    .value()
                {
                    None => write!(f, "null"),
                    Some(buf) => {
                        write!(
                            f,
                            "{}",
                            buf.as_slice().iter().map(|b| format!("{b:?}")).format(",")
                        )
                    }
                }
            }
            DType::Struct(..) => todo!(),
            DType::List(..) => todo!(),
            DType::Extension(..) => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use vortex_buffer::Buffer;
    use vortex_dtype::Nullability::{NonNullable, Nullable};
    use vortex_dtype::{DType, PType};

    use crate::Scalar;

    #[test]
    fn display_bool() {
        assert_eq!(format!("{}", Scalar::from(false)), "false");
        assert_eq!(format!("{}", Scalar::from(true)), "true");
        assert_eq!(format!("{}", Scalar::null(DType::Bool(Nullable))), "null");
    }

    #[test]
    fn display_primitive() {
        assert_eq!(format!("{}", Scalar::from(0_u8)), "0");
        assert_eq!(format!("{}", Scalar::from(255_u8)), "255");

        assert_eq!(format!("{}", Scalar::from(0_u16)), "0");
        assert_eq!(format!("{}", Scalar::from(!0_u16)), "65535");

        assert_eq!(format!("{}", Scalar::from(0_u32)), "0");
        assert_eq!(format!("{}", Scalar::from(!0_u32)), "4294967295");

        assert_eq!(format!("{}", Scalar::from(0_u64)), "0");
        assert_eq!(format!("{}", Scalar::from(!0_u64)), "18446744073709551615");

        assert_eq!(
            format!("{}", Scalar::null(DType::Primitive(PType::U8, Nullable))),
            "null"
        );
    }

    #[test]
    fn display_utf8() {
        assert_eq!(format!("{}", Scalar::from("Hello World!")), "Hello World!");
        assert_eq!(format!("{}", Scalar::null(DType::Utf8(Nullable))), "null");
    }

    #[test]
    fn display_binary() {
        assert_eq!(
            format!(
                "{}",
                Scalar::binary(Buffer::from("Hello World!".as_bytes()), NonNullable)
            ),
            "48656c6c6f20576f726c6421"
        );
        assert_eq!(format!("{}", Scalar::null(DType::Binary(Nullable))), "null");
    }
}
