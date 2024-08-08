use vortex_buffer::{Buffer, BufferString};
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexError};
use vortex_proto::scalar as pb;
use vortex_proto::scalar::scalar_value::Kind;
use vortex_proto::scalar::ListValue;

use crate::pvalue::PValue;
use crate::{Scalar, ScalarValue};

impl From<&Scalar> for pb::Scalar {
    fn from(value: &Scalar) -> Self {
        pb::Scalar {
            dtype: Some((&value.dtype).into()),
            value: Some((&value.value).into()),
        }
    }
}

impl From<&ScalarValue> for pb::ScalarValue {
    fn from(value: &ScalarValue) -> Self {
        match value {
            ScalarValue::Null => pb::ScalarValue {
                kind: Some(Kind::NullValue(0)),
            },
            ScalarValue::Bool(v) => pb::ScalarValue {
                kind: Some(Kind::BoolValue(*v)),
            },
            ScalarValue::Primitive(v) => v.into(),
            ScalarValue::Buffer(v) => pb::ScalarValue {
                kind: Some(Kind::BytesValue(v.as_slice().to_vec())),
            },
            ScalarValue::BufferString(v) => pb::ScalarValue {
                kind: Some(Kind::StringValue(v.as_str().to_string())),
            },
            ScalarValue::List(v) => {
                let mut values = Vec::with_capacity(v.len());
                for elem in v.iter() {
                    values.push(pb::ScalarValue::from(elem));
                }
                pb::ScalarValue {
                    kind: Some(Kind::ListValue(ListValue { values })),
                }
            }
        }
    }
}

impl From<&PValue> for pb::ScalarValue {
    fn from(value: &PValue) -> Self {
        match value {
            PValue::I8(v) => pb::ScalarValue {
                kind: Some(Kind::Int32Value(*v as i32)),
            },
            PValue::I16(v) => pb::ScalarValue {
                kind: Some(Kind::Int32Value(*v as i32)),
            },
            PValue::I32(v) => pb::ScalarValue {
                kind: Some(Kind::Int32Value(*v)),
            },
            PValue::I64(v) => pb::ScalarValue {
                kind: Some(Kind::Int64Value(*v)),
            },
            PValue::U8(v) => pb::ScalarValue {
                kind: Some(Kind::Uint32Value(*v as u32)),
            },
            PValue::U16(v) => pb::ScalarValue {
                kind: Some(Kind::Uint32Value(*v as u32)),
            },
            PValue::U32(v) => pb::ScalarValue {
                kind: Some(Kind::Uint32Value(*v)),
            },
            PValue::U64(v) => pb::ScalarValue {
                kind: Some(Kind::Uint64Value(*v)),
            },
            PValue::F16(v) => pb::ScalarValue {
                kind: Some(Kind::FloatValue(v.to_f32())),
            },
            PValue::F32(v) => pb::ScalarValue {
                kind: Some(Kind::FloatValue(*v)),
            },
            PValue::F64(v) => pb::ScalarValue {
                kind: Some(Kind::DoubleValue(*v)),
            },
        }
    }
}

impl TryFrom<&pb::Scalar> for Scalar {
    type Error = VortexError;

    fn try_from(value: &pb::Scalar) -> Result<Self, Self::Error> {
        let dtype = DType::try_from(
            value
                .dtype
                .as_ref()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Scalar missing dtype"))?,
        )?;

        let value = ScalarValue::try_from(
            value
                .value
                .as_ref()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Scalar missing value"))?,
        )?;

        Ok(Self { dtype, value })
    }
}

impl TryFrom<&pb::ScalarValue> for ScalarValue {
    type Error = VortexError;

    fn try_from(value: &pb::ScalarValue) -> Result<Self, Self::Error> {
        let kind = value
            .kind
            .as_ref()
            .ok_or_else(|| vortex_err!(InvalidSerde: "ScalarValue missing kind"))?;

        Ok(match kind {
            Kind::NullValue(_) => ScalarValue::Null,
            Kind::BoolValue(v) => ScalarValue::Bool(*v),
            Kind::Int32Value(v) => ScalarValue::Primitive(PValue::I32(*v)),
            Kind::Int64Value(v) => ScalarValue::Primitive(PValue::I64(*v)),
            Kind::Uint32Value(v) => ScalarValue::Primitive(PValue::U32(*v)),
            Kind::Uint64Value(v) => ScalarValue::Primitive(PValue::U64(*v)),
            Kind::FloatValue(v) => ScalarValue::Primitive(PValue::F32(*v)),
            Kind::DoubleValue(v) => ScalarValue::Primitive(PValue::F64(*v)),
            Kind::StringValue(v) => ScalarValue::BufferString(BufferString::from(v.clone())),
            Kind::BytesValue(v) => ScalarValue::Buffer(Buffer::from(v.as_slice())),
            Kind::ListValue(v) => {
                let mut values = Vec::with_capacity(v.values.len());
                for elem in v.values.iter() {
                    values.push(ScalarValue::try_from(elem)?);
                }
                ScalarValue::List(values.into())
            }
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex_buffer::BufferString;
    use vortex_dtype::PType::I32;
    use vortex_dtype::{DType, Nullability};
    use vortex_proto::scalar as pb;

    use crate::{Scalar, ScalarValue};

    fn round_trip(scalar: Scalar) {
        Scalar::try_from(&pb::Scalar::from(&scalar)).unwrap();
    }

    #[test]
    fn test_null() {
        round_trip(Scalar::null(DType::Null));
    }

    #[test]
    fn test_bool() {
        round_trip(Scalar::new(
            DType::Bool(Nullability::Nullable),
            ScalarValue::Bool(true),
        ));
    }

    #[test]
    fn test_primitive() {
        round_trip(Scalar::new(
            DType::Primitive(I32, Nullability::Nullable),
            ScalarValue::Primitive(42i32.into()),
        ));
    }

    #[test]
    fn test_buffer() {
        round_trip(Scalar::new(
            DType::Binary(Nullability::Nullable),
            ScalarValue::Buffer(vec![1, 2, 3].into()),
        ));
    }

    #[test]
    fn test_buffer_string() {
        round_trip(Scalar::new(
            DType::Utf8(Nullability::Nullable),
            ScalarValue::BufferString(BufferString::from("hello".to_string())),
        ));
    }

    #[test]
    fn test_list() {
        round_trip(Scalar::new(
            DType::List(
                Arc::new(DType::Primitive(I32, Nullability::Nullable)),
                Nullability::Nullable,
            ),
            ScalarValue::List(
                vec![
                    ScalarValue::Primitive(42i32.into()),
                    ScalarValue::Primitive(43i32.into()),
                ]
                .into(),
            ),
        ));
    }
}
