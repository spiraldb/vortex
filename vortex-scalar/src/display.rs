use std::fmt::{Display, Formatter};

use itertools::Itertools;
use vortex_dtype::{match_each_native_ptype, DType};

use crate::binary::BinaryScalar;
use crate::bool::BoolScalar;
use crate::primitive::PrimitiveScalar;
use crate::struct_::StructScalar;
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
                            buf.as_slice().iter().map(|b| format!("{b:x}")).format(",")
                        )
                    }
                }
            }
            DType::Struct(dtype, _) => {
                let v = StructScalar::try_from(self).map_err(|_| std::fmt::Error)?;

                if v.is_null() {
                    write!(f, "null")
                } else {
                    write!(f, "{{")?;
                    let formatted_fields = dtype
                        .names()
                        .iter()
                        .enumerate()
                        .map(|(idx, name)| match v.field_by_idx(idx) {
                            None => format!("{name}:null"),
                            Some(val) => format!("{name}:{val}"),
                        })
                        .format(",");
                    write!(f, "{}", formatted_fields)?;
                    write!(f, "}}")
                }
            }
            DType::List(..) => todo!(),
            DType::Extension(..) => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vortex_buffer::Buffer;
    use vortex_dtype::Nullability::{NonNullable, Nullable};
    use vortex_dtype::{DType, PType, StructDType};

    use crate::{PValue, Scalar, ScalarValue};

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
            "48,65,6c,6c,6f,20,57,6f,72,6c,64,21"
        );
        assert_eq!(format!("{}", Scalar::null(DType::Binary(Nullable))), "null");
    }

    #[test]
    fn display_empty_struct() {
        fn dtype() -> DType {
            DType::Struct(StructDType::new(Arc::new([]), vec![]), Nullable)
        }

        assert_eq!(format!("{}", Scalar::null(dtype())), "null");

        assert_eq!(format!("{}", Scalar::r#struct(dtype(), vec![])), "{}");
    }

    #[test]
    fn display_one_field_struct() {
        fn dtype() -> DType {
            DType::Struct(
                StructDType::new(
                    Arc::new([Arc::from("foo")]),
                    vec![DType::Primitive(PType::U32, Nullable)],
                ),
                Nullable,
            )
        }

        assert_eq!(format!("{}", Scalar::null(dtype())), "null");

        assert_eq!(
            format!("{}", Scalar::r#struct(dtype(), vec![ScalarValue::Null])),
            "{foo:null}"
        );

        assert_eq!(
            format!(
                "{}",
                Scalar::r#struct(dtype(), vec![ScalarValue::Primitive(PValue::U32(32))])
            ),
            "{foo:32}"
        );
    }

    #[test]
    fn display_two_field_struct() {
        fn dtype() -> DType {
            DType::Struct(
                StructDType::new(
                    Arc::new([Arc::from("foo"), Arc::from("bar")]),
                    vec![
                        DType::Bool(Nullable),
                        DType::Primitive(PType::U32, Nullable),
                    ],
                ),
                Nullable,
            )
        }

        assert_eq!(format!("{}", Scalar::null(dtype())), "null");

        assert_eq!(
            format!("{}", Scalar::r#struct(dtype(), vec![])),
            "{foo:null,bar:null}"
        );

        assert_eq!(
            format!(
                "{}",
                Scalar::r#struct(dtype(), vec![ScalarValue::Bool(true)])
            ),
            "{foo:true,bar:null}"
        );

        assert_eq!(
            format!(
                "{}",
                Scalar::r#struct(
                    dtype(),
                    vec![
                        ScalarValue::Bool(true),
                        ScalarValue::Primitive(PValue::U32(32))
                    ]
                )
            ),
            "{foo:true,bar:32}"
        );
    }
}
