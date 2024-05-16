#![cfg(feature = "proto")]

use prost_types::value::Kind;
use prost_types::{ListValue, Struct, Value};
use vortex_buffer::BufferString;
use vortex_dtype::{DType, StructDType};
use vortex_error::{vortex_bail, vortex_err, VortexError};

use crate::pvalue::PValue;
use crate::{proto::scalar as pb, Scalar, ScalarValue};

impl TryFrom<&pb::Scalar> for Scalar {
    type Error = VortexError;

    fn try_from(value: &pb::Scalar) -> Result<Self, Self::Error> {
        let dtype = DType::try_from(
            value
                .dtype
                .as_ref()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Scalar missing dtype"))?,
        )?;

        let scalar_value = value
            .value
            .as_ref()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Scalar missing value"))?;

        let pb_value = scalar_value
            .value
            .as_ref()
            .ok_or_else(|| vortex_err!(InvalidSerde: "ScalarValue missing value"))?;

        let value = try_from_value(&dtype, pb_value)?;

        Ok(Self { dtype, value })
    }
}

fn try_from_value(dtype: &DType, value: &Value) -> Result<ScalarValue, VortexError> {
    let kind = value
        .kind
        .as_ref()
        .ok_or_else(|| vortex_err!(InvalidSerde: "Value missing kind"))?;

    Ok(match kind {
        Kind::NullValue(_) => {
            if !dtype.is_nullable() {
                vortex_bail!(InvalidSerde: "Expected a nullable or Null dtype, found {:?}", dtype);
            }

            ScalarValue::Null
        }
        Kind::BoolValue(v) => {
            if !matches!(dtype, DType::Bool(_)) {
                vortex_bail!(InvalidSerde: "Expected a bool dtype, found {:?}", dtype);
            }

            ScalarValue::Bool(*v)
        }
        Kind::NumberValue(v) => {
            if !matches!(dtype, DType::Primitive(_, _)) {
                vortex_bail!(InvalidSerde: "Expected a primitive dtype, found {:?}", dtype);
            }

            ScalarValue::Primitive(PValue::F64(*v))
        }
        Kind::StringValue(v) => {
            if !matches!(dtype, DType::Utf8(_)) {
                vortex_bail!(InvalidSerde: "Expected a utf8 dtype, found {:?}", dtype);
            }

            ScalarValue::BufferString(BufferString::from(v.clone()))
        }
        Kind::ListValue(v) => {
            if let DType::List(elem_dtype, _) = dtype {
                return try_from_list_value(elem_dtype, v);
            }

            vortex_bail!(InvalidSerde: "Expected a list dtype, found {:?}", dtype);
        }
        Kind::StructValue(v) => {
            if let DType::Struct(sdt, _) = dtype {
                return try_from_struct_value(sdt, v);
            }

            vortex_bail!(InvalidSerde: "Expected a struct dtype, found {:?}", dtype);
        }
    })
}

fn try_from_list_value(elem_dtype: &DType, value: &ListValue) -> Result<ScalarValue, VortexError> {
    let mut values = vec![];

    for elem in value.values.iter() {
        let nested = try_from_value(elem_dtype, elem)?;

        // Allow null values for nullable list only.
        if matches!(nested, ScalarValue::Null) && !elem_dtype.is_nullable() {
            vortex_bail!(InvalidSerde: "Non-nullable list element is null");
        }

        values.push(try_from_value(elem_dtype, elem)?);
    }

    Ok(ScalarValue::List(values.into()))
}

fn try_from_struct_value(dtype: &StructDType, value: &Struct) -> Result<ScalarValue, VortexError> {
    let mut values = vec![];

    for (field, field_dt) in dtype.names().iter().zip(dtype.dtypes().iter()) {
        if let Some((_, v)) =
            // Add field values in order defined by the struct dtype.
            value
                .fields
                .iter()
                .find(|(f, _)| field.as_ref() == f.as_str())
        {
            let nested = try_from_value(field_dt, v)?;

            // Allow null values for nullable struct only.
            if matches!(nested, ScalarValue::Null) && !field_dt.is_nullable() {
                vortex_bail!(InvalidSerde: "Non-nullable struct field {} is null", field);
            }

            values.push(try_from_value(field_dt, v)?);
        } else if field_dt.is_nullable() {
            values.push(ScalarValue::Null);
        } else {
            vortex_bail!(InvalidSerde: "Non-nullable struct field {} not found", field);
        }
    }

    Ok(ScalarValue::List(values.into()))
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use prost_types::value::Kind;
    use prost_types::Value;
    use vortex_dtype::{DType, FieldNames, Nullability, PType, StructDType};

    use crate::Scalar;
    use crate::{proto as pb, PValue, ScalarValue};

    fn round_trip(dtype: DType, value: Value) -> Scalar {
        let pb_scalar = pb::scalar::Scalar {
            dtype: Some(pb::dtype::DType::from(&dtype)),
            value: Some(pb::scalar::ScalarValue { value: Some(value) }),
        };
        Scalar::try_from(&pb_scalar).unwrap()
    }

    #[test]
    fn test_null() {
        let scalar = round_trip(
            DType::Null,
            Value {
                kind: Some(Kind::NullValue(0)),
            },
        );
        assert_eq!(scalar.value, ScalarValue::Null);
    }

    #[test]
    fn test_nullable() {
        let scalar = round_trip(
            DType::Bool(Nullability::Nullable),
            Value {
                kind: Some(Kind::NullValue(0)),
            },
        );
        assert_eq!(scalar.value, ScalarValue::Null);
    }

    #[test]
    fn test_bool() {
        let scalar = round_trip(
            DType::Bool(Nullability::NonNullable),
            Value {
                kind: Some(Kind::BoolValue(true)),
            },
        );
        assert_eq!(scalar.value, ScalarValue::Bool(true));
    }

    #[test]
    fn test_number() {
        let scalar = round_trip(
            DType::Primitive(PType::F64, Nullability::NonNullable),
            Value {
                kind: Some(Kind::NumberValue(42.42)),
            },
        );
        assert_eq!(scalar.value, ScalarValue::Primitive(PValue::F64(42.42)));
    }

    #[test]
    fn test_string() {
        let scalar = round_trip(
            DType::Utf8(Nullability::NonNullable),
            Value {
                kind: Some(Kind::StringValue("hello".to_string())),
            },
        );
        assert_eq!(
            scalar.value,
            ScalarValue::BufferString("hello".to_string().into())
        );
    }

    #[test]
    fn test_list() {
        let scalar = round_trip(
            DType::List(
                Arc::new(DType::Bool(Nullability::Nullable)),
                Nullability::NonNullable,
            ),
            Value {
                kind: Some(Kind::ListValue(prost_types::ListValue {
                    values: vec![Value {
                        kind: Some(Kind::BoolValue(true)),
                    }],
                })),
            },
        );
        assert_eq!(
            scalar.value,
            ScalarValue::List(vec![ScalarValue::Bool(true)].into())
        );
    }

    #[test]
    fn test_list_nullable() {
        let scalar = round_trip(
            DType::List(
                Arc::new(DType::Bool(Nullability::Nullable)),
                Nullability::Nullable,
            ),
            Value {
                kind: Some(Kind::ListValue(prost_types::ListValue {
                    values: vec![Value {
                        kind: Some(Kind::NullValue(0)),
                    }],
                })),
            },
        );
        assert_eq!(
            scalar.value,
            ScalarValue::List(vec![ScalarValue::Null].into())
        );
    }

    #[test]
    fn test_struct() {
        let names = FieldNames::from(vec![Arc::from("a")]);
        let mut nested_fields = BTreeMap::new();
        nested_fields.insert(
            "a".to_string(),
            Value {
                kind: Some(Kind::BoolValue(true)),
            },
        );

        let scalar = round_trip(
            DType::Struct(
                StructDType::new(names, vec![DType::Bool(Nullability::NonNullable)]),
                Nullability::NonNullable,
            ),
            Value {
                kind: Some(Kind::StructValue(prost_types::Struct {
                    fields: nested_fields,
                })),
            },
        );
        assert_eq!(
            scalar.value,
            ScalarValue::List(vec![ScalarValue::Bool(true)].into())
        );
    }

    #[test]
    fn test_struct_nullable() {
        let names = FieldNames::from(vec![Arc::from("a")]);
        let nested_fields = BTreeMap::new();

        let scalar = round_trip(
            DType::Struct(
                StructDType::new(names, vec![DType::Bool(Nullability::Nullable)]),
                Nullability::NonNullable,
            ),
            Value {
                kind: Some(Kind::StructValue(prost_types::Struct {
                    fields: nested_fields,
                })),
            },
        );
        assert_eq!(
            scalar.value,
            ScalarValue::List(vec![ScalarValue::Null].into())
        );
    }

    #[test]
    fn test_wrong_type() {
        let pb_scalar = pb::scalar::Scalar {
            dtype: Some(pb::dtype::DType::from(&DType::Primitive(
                PType::F64,
                Nullability::NonNullable,
            ))),
            value: Some(pb::scalar::ScalarValue {
                value: Some(Value {
                    kind: Some(Kind::BoolValue(true)),
                }),
            }),
        };
        assert!(Scalar::try_from(&pb_scalar).is_err());
    }
}
